package job

import (
	"fmt"
	"testing"
	"time"

	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/execute"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/statusreport"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/ums"
	"github.comcast.com/mesa/mafaas-types/jobs"
)

func init() {
	ResourcePollFrequency = time.Microsecond * 100
}

func TestLiveJobSuccessfullyRuns(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(100 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "1",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	calledBack := false
	job.Run(func(*Job) { calledBack = true })

	if !calledBack {
		t.Errorf("Job did not call callback function when done")
	}
	if status := job.GetStatus(); status != StatusComplete {
		t.Errorf("Job status was wrong after job complete: expected %v, got %v", StatusComplete, status)
	}
	if now := ums.Now(); now < endTime || now > endTime.Add(time.Second) {
		t.Errorf("Job did not end around the end time: job end time %v, now is %v", endTime.Time(), time.Now())
	}
}

func TestLiveJobReportsStatuses(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(100 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	jobID := "1.25"
	namespace := "just a lonely test"
	request := JobRequest{
		ID:               jobID,
		Namespace:        namespace,
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	job.Run(func(*Job) {})

	if len(statusReporter.Reports) != 3 {
		t.Errorf("Live job did not successfully report the right number of statuses: expected %v, got %v", 5, len(statusReporter.Reports))
	} else {
		for i, report := range statusReporter.Reports {
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct jobID: expected %v, got %v", i, jobID, report.JobID)
			}
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct namespace: expected %v, got %v", i, namespace, report.Namespace)
			}
		}

		if statusReporter.Reports[0].Status.Status == nil {
			t.Errorf("Status report index 0 did not have a status included")
		} else if status := *statusReporter.Reports[0].Status.Status; status != jobs.StatusScheduled {
			t.Errorf("Status report index 0 updated status incorrectly: expected %v, got %v", jobs.StatusScheduled, status)
		}
		if statusReporter.Reports[1].Status.Status == nil {
			t.Errorf("Status report index 1 did not have a status included")
		} else if status := *statusReporter.Reports[1].Status.Status; status != jobs.StatusRunning {
			t.Errorf("Status report index 1 updated status incorrectly: expected %v, got %v", jobs.StatusRunning, status)
		} else if statusReporter.Reports[1].Status.ProcessingStartTime == nil {
			t.Errorf("Status report index 1, which is Running, did not update processingStartTime")
		}
		if statusReporter.Reports[2].Status.Status == nil {
			t.Errorf("Status report index 2 did not have a status included")
		} else if status := *statusReporter.Reports[2].Status.Status; status != jobs.StatusCompleted {
			t.Errorf("Status report index 2 updated status incorrectly: expected %v, got %v", jobs.StatusCompleted, status)
		} else if statusReporter.Reports[2].Status.ProcessingEndTime == nil {
			t.Errorf("Status report index 2, which is Completed, did not update processingEndTime")
		}
	}
}

func TestLiveJobSuccessfullyRunsReconnect(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			}, Error: nil,
		}},
		ExecuteResults: []execute.ExecutorResults{},
	}
	startTime := ums.Now().Add(100 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "1.5",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	calledBack := false
	job.Run(func(*Job) { calledBack = true })

	if !calledBack {
		t.Errorf("Job did not call callback function when done")
	}
	if status := job.GetStatus(); status != StatusComplete {
		t.Errorf("Job status was wrong after job complete: expected %v, got %v", StatusComplete, status)
	}
	if now := ums.Now(); now < endTime || now > endTime.Add(time.Second) {
		t.Errorf("Job did not end around the end time: job end time %v, now is %v", endTime.Time(), time.Now())
	}
}

func TestLiveJobCancelDuringScheduled(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(time.Hour)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "2",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)
	if status := job.GetStatus(); status != StatusScheduled {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusScheduled, status)
	}

	err := job.Update(JobUpdates{Cancel: true})
	if err != nil {
		t.Fatalf("Job Update with Cancel failed: %v", err)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusCancelled {
		t.Errorf("Job status was wrong after job cancelled: expected %v, got %v", StatusCancelled, status)
	}
}

func TestLiveJobCancelDuringScheduledReportsStatuses(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(time.Hour)
	endTime := startTime.Add(100 * time.Millisecond)
	jobID := "2.5"
	namespace := "just a lonely test"
	request := JobRequest{
		ID:               jobID,
		Namespace:        namespace,
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)

	err := job.Update(JobUpdates{Cancel: true})
	if err != nil {
		t.Fatalf("Job Update with Cancel failed: %v", err)
	}

	<-doneChan

	if len(statusReporter.Reports) != 2 {
		t.Errorf("Live job did not successfully report the right number of statuses: expected %v, got %v", 2, len(statusReporter.Reports))
	} else {
		for i, report := range statusReporter.Reports {
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct jobID: expected %v, got %v", i, jobID, report.JobID)
			}
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct namespace: expected %v, got %v", i, namespace, report.Namespace)
			}
		}

		if statusReporter.Reports[0].Status.Status == nil {
			t.Errorf("Status report index 0 did not have a status included")
		} else if status := *statusReporter.Reports[0].Status.Status; status != jobs.StatusScheduled {
			t.Errorf("Status report index 0 updated status incorrectly: expected %v, got %v", jobs.StatusScheduled, status)
		}
		if statusReporter.Reports[1].Status.Status == nil {
			t.Errorf("Status report index 1 did not have a status included")
		} else if status := *statusReporter.Reports[1].Status.Status; status != jobs.StatusCancelled {
			t.Errorf("Status report index 1 updated status incorrectly: expected %v, got %v", jobs.StatusCancelled, status)
		} else if statusReporter.Reports[1].Status.ProcessingEndTime != nil {
			t.Errorf("Status report index 1, which is Cancelled, updated processingEndTime when not expected")
		}
	}
}

func TestJobCancelDuringStarting(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Duration: time.Hour,
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(200 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "3",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 180,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)
	if status := job.GetStatus(); status != StatusStageStarting {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageStarting, status)
	}

	err := job.Update(JobUpdates{Cancel: true})
	if err != nil {
		t.Fatalf("Job Update with Cancel failed: %v", err)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusCancelled {
		t.Errorf("Job status was wrong after job cancelled: expected %v, got %v", StatusCancelled, status)
	}
}

func TestLiveJobCancelDuringRunning(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(100 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "4",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)
	if status := job.GetStatus(); status != StatusStageRunning {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageRunning, status)
	}

	err := job.Update(JobUpdates{Cancel: true})
	if err != nil {
		t.Fatalf("Job Update with Cancel failed: %v", err)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusCancelled {
		t.Errorf("Job status was wrong after job cancelled: expected %v, got %v", StatusCancelled, status)
	}
}

func TestLiveJobCancelDuringRunningReportsStatuses(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(100 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	jobID := "4.5"
	namespace := "just a lonely test"
	request := JobRequest{
		ID:               jobID,
		Namespace:        namespace,
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)

	err := job.Update(JobUpdates{Cancel: true})
	if err != nil {
		t.Fatalf("Job Update with Cancel failed: %v", err)
	}

	<-doneChan

	if len(statusReporter.Reports) != 3 {
		t.Errorf("Live job did not successfully report the right number of statuses: expected %v, got %v", 2, len(statusReporter.Reports))
	} else {
		for i, report := range statusReporter.Reports {
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct jobID: expected %v, got %v", i, jobID, report.JobID)
			}
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct namespace: expected %v, got %v", i, namespace, report.Namespace)
			}
		}

		if statusReporter.Reports[0].Status.Status == nil {
			t.Errorf("Status report index 0 did not have a status included")
		} else if status := *statusReporter.Reports[0].Status.Status; status != jobs.StatusScheduled {
			t.Errorf("Status report index 0 updated status incorrectly: expected %v, got %v", jobs.StatusScheduled, status)
		}
		if statusReporter.Reports[1].Status.Status == nil {
			t.Errorf("Status report index 1 did not have a status included")
		} else if status := *statusReporter.Reports[1].Status.Status; status != jobs.StatusRunning {
			t.Errorf("Status report index 1 updated status incorrectly: expected %v, got %v", jobs.StatusRunning, status)
		} else if statusReporter.Reports[1].Status.ProcessingStartTime == nil {
			t.Errorf("Status report index 1, which is Running, processingStartTime not updated")
		}
		if statusReporter.Reports[2].Status.Status == nil {
			t.Errorf("Status report index 2 did not have a status included")
		} else if status := *statusReporter.Reports[2].Status.Status; status != jobs.StatusCancelled {
			t.Errorf("Status report index 2 updated status incorrectly: expected %v, got %v", jobs.StatusCancelled, status)
		} else if statusReporter.Reports[2].Status.ProcessingEndTime == nil {
			t.Errorf("Status report index 2, which is Cancelled, processingEndTime not updated")
		}
	}
}

func TestJobCancelDuringStopping(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States:      []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
				KillResults: []execute.MockTaskResults{{Duration: time.Second / 2}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(50 * time.Millisecond)
	endTime := startTime.Add(50 * time.Millisecond)
	request := JobRequest{
		ID:               "5",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 25,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 200)
	if status := job.GetStatus(); status != StatusStageStopping {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageStopping, status)
	}

	err := job.Update(JobUpdates{Cancel: true})
	if err != nil {
		t.Fatalf("Job Update with Cancel failed: %v", err)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusCancelled {
		t.Errorf("Job status was wrong after job cancelled: expected %v, got %v", StatusCancelled, status)
	}
}

func TestJobFailedDuringStart(t *testing.T) {
	retErr := fmt.Errorf("test error")
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Duration: 100 * time.Millisecond,
			Task:     nil,
			Error:    retErr,
		}},
	}
	startTime := ums.Now().Add(200 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "6",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 120,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)
	if status := job.GetStatus(); status != StatusStageStarting {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageStarting, status)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusFailed {
		t.Errorf("Job status was wrong after error returned: expected %v, got %v", StatusFailed, status)
	}
}

func TestJobFailedDuringRunningReportsStatuses(t *testing.T) {
	retErr := fmt.Errorf("test error")
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Duration: 100 * time.Millisecond,
			Task:     nil,
			Error:    retErr,
		}},
	}
	startTime := ums.Now().Add(200 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	jobID := "6.5"
	namespace := "just a lonely test"
	request := JobRequest{
		ID:               jobID,
		Namespace:        namespace,
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 120,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)
	if status := job.GetStatus(); status != StatusStageStarting {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageStarting, status)
	}

	<-doneChan

	if len(statusReporter.Reports) != 3 {
		t.Errorf("Live job did not successfully report the right number of statuses: expected %v, got %v", 2, len(statusReporter.Reports))
	} else {
		for i, report := range statusReporter.Reports {
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct jobID: expected %v, got %v", i, jobID, report.JobID)
			}
			if report.JobID != jobID {
				t.Errorf("Status report in index %v not with correct namespace: expected %v, got %v", i, namespace, report.Namespace)
			}
		}

		if statusReporter.Reports[0].Status.Status == nil {
			t.Errorf("Status report index 0 did not have a status included")
		} else if status := *statusReporter.Reports[0].Status.Status; status != jobs.StatusScheduled {
			t.Errorf("Status report index 0 updated status incorrectly: expected %v, got %v", jobs.StatusScheduled, status)
		}
		if statusReporter.Reports[1].Status.Status == nil {
			t.Errorf("Status report index 1 did not have a status included")
		} else if status := *statusReporter.Reports[1].Status.Status; status != jobs.StatusRunning {
			t.Errorf("Status report index 1 updated status incorrectly: expected %v, got %v", jobs.StatusRunning, status)
		} else if statusReporter.Reports[1].Status.ProcessingStartTime == nil {
			t.Errorf("Status report index 1, which is Running, processingStartTime not updated")
		}
		if statusReporter.Reports[2].Status.Status == nil {
			t.Errorf("Status report index 2 did not have a status included")
		} else if status := *statusReporter.Reports[2].Status.Status; status != jobs.StatusError {
			t.Errorf("Status report index 2 updated status incorrectly: expected %v, got %v", jobs.StatusError, status)
		} else if statusReporter.Reports[2].Status.ProcessingEndTime == nil {
			t.Errorf("Status report index 2, which is Cancelled, processingEndTime not updated")
		}
	}
}

func TestLiveJobFailedDuringRunning(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{
					{Duration: 100 * time.Millisecond, State: execute.TaskStateRunning},
					{Duration: time.Hour, State: execute.TaskStateFailed, FailedReason: execute.FailedReasonJobFailed},
				},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(50 * time.Millisecond)
	endTime := startTime.Add(200 * time.Millisecond)
	request := JobRequest{
		ID:               "7",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 25,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 100)
	if status := job.GetStatus(); status != StatusStageRunning {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageRunning, status)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusFailed {
		t.Errorf("Job status was wrong after error returned: expected %v, got %v", StatusFailed, status)
	}
}

func TestJobFailureDuringStoppingNotFailed(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States:      []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
				KillResults: []execute.MockTaskResults{{Duration: time.Second / 2}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(50 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "7",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 25,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(time.Millisecond * 200)
	if status := job.GetStatus(); status != StatusStageStopping {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageStopping, status)
	}

	<-doneChan
	if status := job.GetStatus(); status != StatusComplete {
		t.Errorf("Job status was wrong after error returned: expected %v, got %v", StatusComplete, status)
	}
}

func TestLiveJobUpdateEndTime(t *testing.T) {
	jobStorer := &MockJobStorer{Jobs: map[string]string{}}
	statusReporter := &statusreport.MockStatusReporter{}
	executor := &execute.MockExecutor{
		ReconnectResults: []execute.ExecutorResults{{Task: nil, Error: nil}},
		ExecuteResults: []execute.ExecutorResults{{
			Task: &execute.MockTask{
				States: []execute.MockTaskState{{Duration: time.Hour, State: execute.TaskStateRunning}},
			},
			Error: nil,
		}},
	}
	startTime := ums.Now().Add(100 * time.Millisecond)
	endTime := startTime.Add(100 * time.Millisecond)
	request := JobRequest{
		ID:               "8",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}
	job := NewJob(request, executor, jobStorer, statusReporter)

	doneChan := make(chan bool)
	go job.Run(func(*Job) { doneChan <- true })

	time.Sleep(150 * time.Millisecond)
	if status := job.GetStatus(); status != StatusStageRunning {
		t.Fatalf("Job status was wrong: expected %v, got %v", StatusStageRunning, status)
	}

	newEndTime := endTime.Add(100 * time.Millisecond)
	err := job.Update(JobUpdates{EndTime: &newEndTime})
	if err != nil {
		t.Fatalf("Job Update with EndTime failed: %v", err)
	}

	<-doneChan
	if now := ums.Now(); now < newEndTime || now > newEndTime.Add(time.Second) {
		t.Errorf("Job did not end around the new end time: job end time %v, now is %v", newEndTime.Time(), time.Now())
	}
}
