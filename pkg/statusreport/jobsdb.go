package statusreport

import (
	"context"

	"github.comcast.com/mesa/mafaas-common/db/jobsdb"
)

type JobsDBStatusReporter struct {
	Client     jobsdb.Client
	NumRetries int
}

func (r *JobsDBStatusReporter) ReportStatus(jobID, namespace string, status JobStatus) error {
	if status.Status == nil && status.ProcessingStartTime == nil && status.ProcessingEndTime == nil {
		return nil
	}

	updateData := map[string]any{}
	if status.Status != nil {
		updateData[jobsdb.JobStatusField] = *status.Status
	}
	if status.ProcessingStartTime != nil {
		updateData[jobsdb.ProcessingStartTimeField] = *status.ProcessingStartTime
	}
	if status.ProcessingEndTime != nil {
		updateData[jobsdb.ProcessingEndTimeField] = *status.ProcessingEndTime
	}

	_, dbErr := r.Client.UpdateJob(context.Background(), &jobsdb.UpdateJobRequest{
		JobId:      jobID,
		Namespace:  namespace,
		Retries:    r.NumRetries,
		UpdateData: updateData,
	})

	if dbErr != nil && dbErr.HasError() {
		return dbErr.Error
	}
	return nil
}
