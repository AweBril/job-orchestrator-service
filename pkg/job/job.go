package job

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/execute"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/orchestratorlog"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/statusreport"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/ums"
	"github.comcast.com/mesa/mafaas-types/jobs"
	"github.comcast.com/mesa/mafslog/ecslog"
	"golang.org/x/exp/slog"
)

type JobType string

const (
	JobTypeLive JobType = "live"
	JobTypeFile JobType = "file"
)

var ResourcePollFrequency = 30 * time.Second

type JobRequest struct {
	ID               string         `json:"id"`
	Namespace        string         `json:"namespace"`
	Type             JobType        `json:"jobType"`
	Outputs          []string       `json:"outputs"`
	Params           StageParams    `json:"params"`
	MaxStartDuration ums.UnixMillis `json:"maxStartDuration"`
	StartTime        ums.UnixMillis `json:"startTime"`
	EndTime          ums.UnixMillis `json:"endTime"`
}

type JobStatus string

const (
	StatusScheduled     = JobStatus("scheduled")
	StatusQueued        = JobStatus("queued")
	StatusStageStarting = JobStatus("stage_starting")
	StatusStageRunning  = JobStatus("stage_running")
	StatusStageComplete = JobStatus("stage_complete")
	StatusStageStopping = JobStatus("stage_stopping")
	StatusComplete      = JobStatus("complete")
	StatusCancelled     = JobStatus("cancelled")
	StatusFailed        = JobStatus("failed")
)

var JobStatusOrds = map[JobStatus]int{
	StatusQueued:        0,
	StatusScheduled:     1,
	StatusStageStarting: 2,
	StatusStageRunning:  3,
	StatusStageStopping: 4,
	StatusStageComplete: 5,
	StatusComplete:      10,
	StatusCancelled:     -2,
	StatusFailed:        -3,
}

// Ord gets the ordinal value of this job status, so it can be linearly compared in order.
func (s JobStatus) Ord() int {
	return JobStatusOrds[s]
}

// JobStatusCategory categorizes jobs based on if they have started yet, are in progress, or have finished.
type JobStatusCategory string

const (
	StatusCategoryUnknown    = JobStatusCategory("unknown")
	StatusCategoryWaiting    = JobStatusCategory("waiting")
	StatusCategoryInProgress = JobStatusCategory("in_progress")
	StatusCategoryDone       = JobStatusCategory("done")
)

var (
	WaitingStatuses    = []JobStatus{StatusScheduled, StatusQueued}
	InProgressStatuses = []JobStatus{StatusStageStarting, StatusStageRunning, StatusStageStopping, StatusStageComplete}
	DoneStatuses       = []JobStatus{StatusComplete, StatusCancelled, StatusFailed}
)

// Category tells what category this status is. It is always one of the "StatusCategory" constants defined in this package.
func (s JobStatus) Category() JobStatusCategory {
	for _, status := range WaitingStatuses {
		if s == status {
			return StatusCategoryWaiting
		}
	}

	for _, status := range InProgressStatuses {
		if s == status {
			return StatusCategoryInProgress
		}
	}

	for _, status := range DoneStatuses {
		if s == status {
			return StatusCategoryDone
		}
	}

	return StatusCategoryUnknown
}

// Statuses tells what are all the statuses this category contains. It is always one of the "*Statuses" array variable defined in this package.
func (c JobStatusCategory) Statuses() []JobStatus {
	if c == StatusCategoryWaiting {
		return WaitingStatuses
	}
	if c == StatusCategoryInProgress {
		return InProgressStatuses
	}

	if c == StatusCategoryDone {
		return DoneStatuses
	}

	return []JobStatus{}
}

// Job represents and can run a complete MAF job from start to finish.
type Job struct {
	JobRequest

	Status              JobStatus `json:"status"`
	ProcessingStartTime time.Time `json:"processingStartTime"`
	ProcessingEndTime   time.Time `json:"processingEndTime"`
	CurrentStage        int       `json:"currentStage"`
	stages              []*Stage
	updatableFieldLock  *sync.Mutex
	cancel              context.CancelFunc

	jobStorer      JobStorer
	statusReporter statusreport.StatusReporter
	executor       execute.Executor

	logger *ecslog.Logger
}

// NewJob creates a new job from a job request, and required interfaces.
func NewJob(request JobRequest, executor execute.Executor, jobStorer JobStorer, statusReporter statusreport.StatusReporter) *Job {
	job := &Job{
		JobRequest:   request,
		CurrentStage: 1,
	}
	job.Setup(executor, jobStorer, statusReporter)
	switch request.Type {
	case JobTypeFile:
		job.SetStatus(StatusQueued)
	case JobTypeLive:
		job.SetStatus(StatusScheduled)
	}

	return job
}

// Setup sets up private state variables, which is useful for resuming from external JSON that doesn't save the private
// state variables.
func (j *Job) Setup(executor execute.Executor, jobStorer JobStorer, statusReporter statusreport.StatusReporter) {
	j.executor = executor
	j.statusReporter = statusReporter
	j.jobStorer = jobStorer
	j.updatableFieldLock = &sync.Mutex{}
	j.logger = ecslog.Default().With(j)

	j.stages = []*Stage{{
		Namespace: j.Namespace,
		JobID:     j.ID,
		StageNum:  1,
		Outputs:   j.Outputs,
		Executor:  j.executor,
	}}
	j.stages[0].Setup(j.logger)
}

// ECSLogAttributes returns the log attributes of this type.
func (j *Job) ECSLogAttributes() []slog.Attr {
	return orchestratorlog.MAFJob{Namespace: j.Namespace, JobID: j.ID}.ECSLogAttributes()
}

// Save saves this job in the internally registered jobStorer.
func (j *Job) Save() error {
	return j.jobStorer.UpdateJob(j)
}

// GetStatus is the concurrency-safe way to get the current status of this job.
func (j *Job) GetStatus() JobStatus {
	j.updatableFieldLock.Lock()
	defer j.updatableFieldLock.Unlock()
	return j.Status
}

// SetStatus is the concurrency-safe way to set the current status of this job. It sets the status in multiple places:
// 1. in memory
// 2. remote storage
// 3. the statusReporter
//
// The statusReporter has some additional information added based on the status change:
// 1. All statuses are boiled down to their counterpart as in the mafaas common job status
// 2. If the state change moves from "waiting" to "running", this is considered  "processing start time"
// 3. If the state change moves from "running" to "done", this is considered the "processing end time"
func (j *Job) SetStatus(status JobStatus) {
	j.updatableFieldLock.Lock()
	prevStatus := j.Status
	j.Status = status
	j.updatableFieldLock.Unlock()

	statusUpdate := statusreport.JobStatus{}
	statusPtr := func(s jobs.Status) *jobs.Status {
		return &s
	}
	switch j.GetStatus() {
	case StatusQueued:
		statusUpdate.Status = statusPtr(jobs.StatusQueued)
	case StatusScheduled:
		statusUpdate.Status = statusPtr(jobs.StatusScheduled)
	case StatusComplete:
		statusUpdate.Status = statusPtr(jobs.StatusCompleted)
	case StatusCancelled:
		statusUpdate.Status = statusPtr(jobs.StatusCancelled)
	case StatusFailed:
		statusUpdate.Status = statusPtr(jobs.StatusError)
	default:
		if prevStatus.Category() == StatusCategoryWaiting {
			statusUpdate.Status = statusPtr(jobs.StatusRunning)
			j.ProcessingStartTime = time.Now()
			statusUpdate.ProcessingStartTime = &j.ProcessingStartTime
		}
	}
	if status.Category() == StatusCategoryDone && prevStatus.Category() == StatusCategoryInProgress {
		j.ProcessingEndTime = time.Now()
		statusUpdate.ProcessingEndTime = &j.ProcessingEndTime
	}

	if statusUpdate.Status != nil ||
		statusUpdate.ProcessingStartTime != nil ||
		statusUpdate.ProcessingEndTime != nil {
		err := j.statusReporter.ReportStatus(j.ID, j.Namespace, statusUpdate)
		if err != nil {
			j.logger.Error("failed to report status", http.StatusInternalServerError, err)
		}
	}

	j.Save()
}

// GetEndTime is the concurrency-safe way to get the current endTime of this job.
func (j *Job) GetEndTime() ums.UnixMillis {
	j.updatableFieldLock.Lock()
	defer j.updatableFieldLock.Unlock()
	return j.EndTime
}

// SetEndTime is the concurrency-safe way to set the current endTime of this job. Because this is a state change, it is
// also saved to storage.
func (j *Job) SetEndTime(endTime ums.UnixMillis) {
	j.updatableFieldLock.Lock()
	j.EndTime = endTime
	j.updatableFieldLock.Unlock()
	j.Save()
}

// registerInterrupt registers the given cancel function to be called by Update. It is nearly impossible for a job to be
// running but not have a cancel function registered, which guarantees that Stop will stop the currently running Run
// thread.
func (j *Job) registerInterrupt(cancel context.CancelFunc) {
	j.updatableFieldLock.Lock()
	defer j.updatableFieldLock.Unlock()
	j.cancel = cancel
}

// unregisterInterrupt removes the current register function, because it is no longer needed. It also calls the cancel
// function, so this can be safely deferred instead of the cancel function directly to clean up resources from the context.
func (j *Job) unregisterInterrupt() {
	j.interrupt()
}

// interrupt interrupts the currently running job. Before this is done, the state should have been changed by the
// calling thread so that the currently running Run() thread can detect those changes.
func (j *Job) interrupt() {
	j.updatableFieldLock.Lock()
	defer j.updatableFieldLock.Unlock()
	if j.cancel != nil {
		j.cancel()
		j.cancel = nil
	}
}

// Run is the primary function responsible for running and managing this job.
//
// First, it will block until the appropriate time to begin allocating resources- if this job is in File mode,
// then it will enter "queued" and wait until Update is called with StartQueued=true. If this job is in Live mode,
// then it will wait until the specified amount of time before the StartTime so that the resources can be available at
// the given StartTime.
//
// Then, it will run all of the stages that comprises this job in order. Each of those stages follows the pattern:
//
// startStage -> manageRunningStage -> shutdown
//
// Where any errors will abort, and it will abort if the job is cancelled. "shutdown" is always called if a stage has
// been started, to ensure that the resources are cleaned up.
//
// This function is not thread safe- it should only ever be called once in the lifetime of this job, unless this job
// is being restarted if the thread was lost (for example, if this program crashes and restarts).
func (j *Job) Run(doneCallback func(*Job)) {
	defer doneCallback(j)

	j.logger.Info(fmt.Sprintf("running as %s job", j.Type))

	if j.GetStatus() == StatusScheduled {
		j.waitForScheduledTime()
	}
	if j.GetStatus() == StatusQueued {
		j.waitForQueue()
	}

	for j.GetStatus().Category() != StatusCategoryDone && j.CurrentStage <= len(j.stages) {
		if j.GetStatus().Ord() >= StatusStageStarting.Ord() && j.GetStatus().Ord() < StatusStageStopping.Ord() {
			j.startStage()
		}
		if j.GetStatus() == StatusStageRunning {
			j.manageRunningStage()
		}

		if curStatus := j.GetStatus(); curStatus == StatusStageStopping || curStatus.Category() == StatusCategoryDone {
			j.shutdownStage()
		}

		if j.GetStatus() == StatusStageComplete {
			j.CurrentStage++
			if j.CurrentStage <= len(j.stages) {
				j.SetStatus(StatusStageStarting)
			}
		}
	}

	if j.GetStatus().Category() != StatusCategoryDone {
		j.SetStatus(StatusComplete)
	}
}

// waitForScheduledTime waits until the appropriate amount of time before the scheduled time so that the tasks can be
// started and ready when the scheduled time comes.
func (j *Job) waitForScheduledTime() {
	timeToWait := time.Until(j.StartTime.Time()) - j.MaxStartDuration.Duration()
	ctx, cancel := context.WithCancel(context.Background())
	j.registerInterrupt(cancel)
	defer j.unregisterInterrupt()
	timer := time.NewTimer(timeToWait)
	defer timer.Stop()

	j.logger.Info(fmt.Sprintf("waiting for scheduled time %v", j.StartTime.Time()))
	select {
	case <-timer.C:
		if j.GetStatus() == StatusScheduled {
			j.SetStatus(StatusStageStarting)
		}
	case <-ctx.Done():
		if j.GetStatus() == StatusScheduled { // Start time was updated, job was not cancelled
			j.waitForScheduledTime()
		}
	}
}

// waitForQueue waits until notified that there are available resources to use by the Update function.
func (j *Job) waitForQueue() {
	ctx, cancel := context.WithCancel(context.Background())
	j.registerInterrupt(cancel)
	defer j.unregisterInterrupt()

	<-ctx.Done()

	if j.GetStatus() == StatusQueued {
		j.SetStatus(StatusStageStarting)
	}
}

// startStage calls the current stage's Start function with the correct parameters and also manages the state changes
// that might occur as a result.
func (j *Job) startStage() {
	ctx, cancel := context.WithTimeout(context.Background(), j.MaxStartDuration.Duration())
	j.registerInterrupt(cancel)
	defer j.unregisterInterrupt()

	err := j.stages[j.CurrentStage-1].Start(ctx, j.Params.Merge(StageParams{
		"global": {"StartTime": j.StartTime.String()},
	}))
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			j.logger.Error(fmt.Sprintf("Failed to start tasks for stage %d", j.CurrentStage), http.StatusInternalServerError, err)
			j.SetStatus(StatusFailed)
		}
	}
	if j.GetStatus() == StatusStageStarting {
		j.SetStatus(StatusStageRunning)
	}
}

// manageRunningStage calls the current stage's ManageRunningX function, where X is this job's type. It calls that
// function with the correct parameters and also manages the state changes that might occur as a result.
func (j *Job) manageRunningStage() {
	ctx, cancel := context.WithCancel(context.Background())
	j.registerInterrupt(cancel)
	defer j.unregisterInterrupt()

	var err error
	if j.Type == JobTypeFile {
		err = j.stages[j.CurrentStage-1].ManageRunningFile(ctx)
	} else {
		err = j.stages[j.CurrentStage-1].ManageRunningLive(ctx, j.GetEndTime)
	}

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			j.logger.Error("Encountered issue while running job", http.StatusInternalServerError, err)
			j.SetStatus(StatusFailed)
		}
	}
	if j.GetStatus() == StatusStageRunning {
		j.SetStatus(StatusStageStopping)
	}
}

// shutdownStage calls the current stage's Shutdown function with the correct parameters and also manages the state
// changes that might occur as a result.
func (j *Job) shutdownStage() {
	j.logger.Info("Shutting down stage", j.stages[j.CurrentStage-1])
	err := j.stages[j.CurrentStage-1].Shutdown(context.TODO())
	if err != nil {
		j.logger.Error("Failed to clean up resources", http.StatusInternalServerError, err)
	}
	if j.GetStatus() == StatusStageStopping {
		j.SetStatus(StatusStageComplete)
	}
}

// JobUpdates is a list of optional updates to apply to a job.
type JobUpdates struct {
	// A non-zero StartTime updates the StartTime of a Live job. Fails if the job is not Live, or has already started.
	StartTime *ums.UnixMillis `json:"startTime,omitempty"`

	// A non-zero EndTime updates the EndTime of this job. Fails if the job is not Live.
	EndTime *ums.UnixMillis `json:"endTIme,omitempty"`

	// A non-zero Parameters updates the Parameters of this job. Fails if the job has already started.
	Parameters StageParams `json:"parameters"`

	// StartQueued starts a job that is in the state "queued". Fails if the job is not in that state.
	StartQueued bool `json:"-"`

	// Cancel cancels a job that has yet to finish. Fails if the job is finished already.
	Cancel bool `json:"-"`
}

// Update updates this job based on the JobUpdates passed in. Only non-zero values are applied, meaning nil values or
// false values are not considered.
func (j *Job) Update(updates JobUpdates) error {
	interrupt := false

	if updates.StartTime != nil {
		if j.Type != JobTypeLive {
			return fmt.Errorf("job is not Live, cannot update start time")
		}
		if j.GetStatus() != StatusScheduled {
			return fmt.Errorf("job has already started, cannot update start time")
		}
		j.updatableFieldLock.Lock()
		j.StartTime = *updates.StartTime
		j.updatableFieldLock.Unlock()
		j.Save()
		j.logger.Info(fmt.Sprintf("Start time updated to %v", j.StartTime.Time()))
		interrupt = true
	}
	if updates.Parameters != nil {
		if status := j.GetStatus(); status != StatusScheduled && status != StatusQueued {
			return fmt.Errorf("job has already started, cannot update parameters")
		}
		j.updatableFieldLock.Lock()
		j.Params = updates.Parameters
		j.updatableFieldLock.Unlock()
		j.Save()
	}
	if updates.EndTime != nil {
		if j.Type != JobTypeLive {
			return fmt.Errorf("job is not Live, cannot update end time")
		}
		j.logger.Info(fmt.Sprintf("End time updated to %v", j.EndTime.Time()))
		j.SetEndTime(*updates.EndTime)
	}
	if updates.StartQueued {
		if j.GetStatus() != StatusQueued {
			return fmt.Errorf("only queued jobs can be started from queue")
		}
		interrupt = true
	}
	if updates.Cancel {
		if j.GetStatus().Category() == StatusCategoryDone {
			return fmt.Errorf("can only cancel jobs that are not done")
		}
		j.SetStatus(StatusCancelled)
		interrupt = true
	}

	if interrupt {
		j.interrupt()
	}

	return nil
}
