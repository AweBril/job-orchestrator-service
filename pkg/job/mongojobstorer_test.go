package job

import (
	"testing"
	"time"

	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/execute"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/statusreport"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/ums"
	"github.comcast.com/mesa/mafslog/ecslog"
)

func TestMongoJobStorerLocally(t *testing.T) {
	uri := "mongodb://localhost:27017/?authSource=admin"

	jobStorer, err := NewMongoJobStorer(uri, "jobDb", time.Second*50, "orchestratorjobs", "abcd", 5)
	if err != nil {
		t.Errorf("Error in creating mongo job storer: %v", err)
	}

	_, dbErr := jobStorer.GetJob("1")
	if dbErr != nil {
		t.Errorf("Error in getting Job details: %v", dbErr)
	}

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
		ID:               "2",
		Type:             JobTypeLive,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}

	updateJob1 := NewJob(request, executor, jobStorer, statusReporter)
	updateJob1.Status = StatusQueued

	dbErr = jobStorer.UpdateJob(updateJob1)
	if dbErr != nil {
		t.Errorf("Error in updating Job details: %v", dbErr)
	}

	request2 := JobRequest{
		ID:               "3",
		Type:             JobTypeFile,
		Outputs:          []string{"test"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 50,
	}

	updateJob2 := NewJob(request2, executor, jobStorer, statusReporter)
	updateJob2.Status = StatusFailed

	dbErr = jobStorer.UpdateJob(updateJob2)
	if dbErr != nil {
		t.Errorf("Error in updating Job details: %v", dbErr)
	}

	request3 := JobRequest{
		ID:               "4",
		Type:             JobTypeFile,
		Outputs:          []string{"test-role"},
		Params:           StageParams{},
		StartTime:        startTime,
		EndTime:          endTime,
		MaxStartDuration: 100,
	}

	updateJob3 := NewJob(request3, executor, jobStorer, statusReporter)
	updateJob3.Status = StatusCancelled

	dbErr = jobStorer.UpdateJob(updateJob3)
	if dbErr != nil {
		t.Errorf("Error in updating Job details: %v", dbErr)
	}

	gtTime := int64(1670870393756)
	ltTime := int64(1670870393956)
	opts := GetJobsOptions{
		Filters: FilterOptions{
			Statuses: &ComparableFilter[JobStatusCategory]{
				Eq: []JobStatusCategory{
					StatusCategoryWaiting,
					StatusCategoryInProgress,
				},
			},
			StartTime: &NumberFilter{
				Gt: &gtTime,
				Lt: &ltTime,
			},
			Search: &SearchFilter{
				Like: "^223",
			},
		},
	}

	_, err = jobStorer.GetJobs(ecslog.Default(), opts)
	if err != nil {
		t.Errorf("Error in retrieving Jobs details: %v", err)
	}

	dbErr = jobStorer.DeleteJob("1")
	if dbErr != nil {
		t.Errorf("Error in deleting Job: %v", dbErr)
	}
}
