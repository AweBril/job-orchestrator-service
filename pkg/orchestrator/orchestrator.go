package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.comcast.com/mesa/mafaas-common/router"
	"github.comcast.com/mesa/mafaas-common/router/middleware"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/execute"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/job"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/response"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/statusreport"
	"github.comcast.com/mesa/mafslog/ecslog"
)

const (
	ORCHESTRATOR_PATH   = "/job-orchestrator"
	GET_JOBS_ENDPOINT   = ORCHESTRATOR_PATH + "/jobs"
	GET_JOB_ENDPOINT    = ORCHESTRATOR_PATH + "/job/{id}"
	UPDATE_JOB_ENDPOINT = ORCHESTRATOR_PATH + "/job/{id}"
	POST_JOB_ENDPOINT   = ORCHESTRATOR_PATH + "/job"
)

// Orchestrator manages all running MAF jobs, and exposes endpoints to manipulate them.
type Orchestrator struct {
	executor       execute.Executor
	jobStorer      job.JobStorer
	statusReporter statusreport.StatusReporter

	jobs    map[string]*job.Job
	jobLock *sync.Mutex
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(executor execute.Executor, jobStorer job.JobStorer, statusReporter statusreport.StatusReporter) *Orchestrator {
	return &Orchestrator{
		executor:       executor,
		jobStorer:      jobStorer,
		statusReporter: statusReporter,

		jobs:    map[string]*job.Job{},
		jobLock: &sync.Mutex{},
	}
}

// ResumeJobsFromRemote checks for any jobs stored remotely, and starts them if they had not yet finished. This function
// should be called when the orchestrator restarts, so it can reconnect to those jobs.
func (o *Orchestrator) ResumeJobsFromRemote() error {
	jobList, err := o.jobStorer.GetJobs(ecslog.Default(), job.GetJobsOptions{
		Filters: job.FilterOptions{
			Statuses: &job.ComparableFilter[job.JobStatusCategory]{
				Eq: []job.JobStatusCategory{job.StatusCategoryWaiting, job.StatusCategoryInProgress},
			},
		}})
	if err != nil {
		return err
	}
	for _, j := range jobList {
		j.Setup(o.executor, o.jobStorer, o.statusReporter)
		o.Register(j)
		go j.Run(o.Unregister)
	}
	return nil
}

// RegisterRoutes registers this orchestrator's endpoints to the given router.
func (o *Orchestrator) RegisterRoutes(r *router.Router) {
	builder := middleware.NewMiddlewareBuilder(1, nil, nil)

	r.API.Handle(GET_JOBS_ENDPOINT, builder.BuildHandler(http.HandlerFunc(o.getJobsHandler), &middleware.HandlerMiddlewareOptions{
		OTELEnabled:   true,
		RecordSession: true,
		OperationName: "get-jobs",
	})).Methods(http.MethodGet)

	r.API.Handle(GET_JOB_ENDPOINT, builder.BuildHandler(http.HandlerFunc(o.getJobHandler), &middleware.HandlerMiddlewareOptions{
		OTELEnabled:   true,
		RecordSession: true,
		OperationName: "get-job",
	})).Methods(http.MethodGet)

	r.API.Handle(POST_JOB_ENDPOINT, builder.BuildHandler(http.HandlerFunc(o.postJobHandler), &middleware.HandlerMiddlewareOptions{
		OTELEnabled:   true,
		RecordSession: true,
		OperationName: "post-job",
	})).Methods(http.MethodPost)

	r.API.Handle(UPDATE_JOB_ENDPOINT, builder.BuildHandler(http.HandlerFunc(o.updateJobHandler), &middleware.HandlerMiddlewareOptions{
		OTELEnabled:   true,
		RecordSession: true,
		OperationName: "update-job",
	})).Methods(http.MethodPut)

	r.API.Handle(GET_JOB_ENDPOINT, builder.BuildHandler(http.HandlerFunc(o.deleteJobHandler), &middleware.HandlerMiddlewareOptions{
		OTELEnabled:   true,
		RecordSession: true,
		OperationName: "delete-job",
	})).Methods(http.MethodDelete)
}

// Register registers the given job with this orchestrator in a concurrency-safe way.
func (o *Orchestrator) Register(j *job.Job) {
	o.jobLock.Lock()
	defer o.jobLock.Unlock()
	o.jobs[j.ID] = j
}

// Unregister unregisters the given job with this orchestrator in a concurrency-safe way.
func (o *Orchestrator) Unregister(j *job.Job) {
	o.jobLock.Lock()
	defer o.jobLock.Unlock()
	delete(o.jobs, j.ID)
}

// GetJob gets a job by ID from this orchestrator in a concurrency-safe way.
func (o *Orchestrator) GetJob(id string) (*job.Job, bool) {
	o.jobLock.Lock()
	defer o.jobLock.Unlock()
	j, ok := o.jobs[id]
	return j, ok
}

// getJobsHandler returns the list of all registered jobs. These jobs are filtered and sorted by the job.GetJobOptions
// provided in the request.
func (o *Orchestrator) getJobsHandler(w http.ResponseWriter, r *http.Request) {
	logger := ecslog.Req(r)
	optsStr := r.FormValue("opts")
	opts := job.GetJobsOptions{}
	if optsStr != "" {
		json.NewDecoder(bytes.NewBufferString(optsStr)).Decode(&opts)
	}

	jobs, err := o.jobStorer.GetJobs(logger, opts)
	if err != nil {
		logger.Error("failed fetching jobs from job storer", http.StatusInternalServerError, err)
		response.InternalError(w)
		return
	}

	response.JSON(w, http.StatusOK, jobs)
}

// getJobHandler returns the full information of the given job.
func (o *Orchestrator) getJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := mux.Vars(r)["id"]
	j, found := o.GetJob(jobID)
	if !found {
		response.Missing(w, job.Job{}, "ID", jobID)
		return
	}

	response.JSON(w, http.StatusOK, j)
}

// postJobHandler creates and runs a new job with the job.JobRequest provided in the request.
func (o *Orchestrator) postJobHandler(w http.ResponseWriter, r *http.Request) {
	var request job.JobRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		response.InvalidRequest(w, "Bad format")
		return
	}
	r.Body.Close()

	j := job.NewJob(request, o.executor, o.jobStorer, o.statusReporter)
	o.Register(j)
	go j.Run(o.Unregister)

	response.New(w, j)
}

// updateJobHandler updates the job specified by the given ID. It can update the StartTime, EndTime, or Parameters of
// that job based on the job.JobUpdates provided in the request.
//
// Only live jobs can have their start and end times updated. Those start and end times can only be updated if they have
// not already elapsed. Parameters can only be updated if the job has not started yet, and both File and Live jobs can
// have their parameters updated.
func (o *Orchestrator) updateJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := mux.Vars(r)["id"]
	j, found := o.GetJob(jobID)
	if !found {
		response.Missing(w, job.Job{}, "ID", jobID)
		return
	}

	updates := job.JobUpdates{}
	json.NewDecoder(r.Body).Decode(&updates)

	err := j.Update(updates)
	if err != nil {
		response.InvalidRequest(w, fmt.Sprintf("invalid update: %v", err))
		return
	}

	response.Empty(w)
}

// deleteJobHandler cancels the given job.
func (o *Orchestrator) deleteJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := mux.Vars(r)["id"]
	j, found := o.GetJob(jobID)
	if !found {
		response.Missing(w, job.Job{}, "ID", jobID)
		return
	}

	err := j.Update(job.JobUpdates{Cancel: true})
	if err != nil {
		response.InvalidRequest(w, fmt.Sprintf("could not cancel: %v", err))
		return
	}

	response.Empty(w)
}
