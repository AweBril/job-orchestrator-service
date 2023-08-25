package statusreport

import (
	"fmt"
	"time"

	"github.comcast.com/mesa/mafaas-types/jobs"
)

type JobStatus struct {
	Status              *jobs.Status
	ProcessingStartTime *time.Time
	ProcessingEndTime   *time.Time
}

type StatusReporter interface {
	ReportStatus(jobID, namespace string, status JobStatus) error
}

type Reporters []StatusReporter

func (rs Reporters) ReportStatus(jobID, namespace string, status JobStatus) error {
	errorList := []error{}
	for _, r := range rs {
		err := r.ReportStatus(jobID, namespace, status)
		if err != nil {
			errorList = append(errorList, err)
		}
	}

	if len(errorList) > 0 {
		return fmt.Errorf("one of the reporters returned an error: %v", errorList)
	}
	return nil
}
