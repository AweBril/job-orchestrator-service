package statusreport

type MockReport struct {
	JobID     string
	Namespace string
	Status    JobStatus
}

type MockStatusReporter struct {
	ReturnedErrors []error
	Reports        []MockReport
}

func (r *MockStatusReporter) ReportStatus(jobID, namespace string, status JobStatus) (err error) {
	defer func() {
		if err == nil {
			r.Reports = append(r.Reports, MockReport{JobID: jobID, Namespace: namespace, Status: status})
		}
	}()

	if len(r.ReturnedErrors) == 0 {
		return
	}
	err = r.ReturnedErrors[0]
	r.ReturnedErrors = r.ReturnedErrors[1:]
	return
}
