package job

import (
	"bytes"
	"encoding/json"

	"github.comcast.com/mesa/mafslog/ecslog"
)

type MockJobStorer struct {
	Jobs map[string]string
}

func NewMockJobStorer() *MockJobStorer {
	return &MockJobStorer{
		Jobs: map[string]string{},
	}
}

func (js *MockJobStorer) UpdateJob(job *Job) error {
	js.Jobs[job.ID] = stringify(job)
	return nil
}

func (js *MockJobStorer) GetJobs(logger *ecslog.Logger, opts GetJobsOptions) ([]*Job, error) {
	jobs := []*Job{}

	for _, j := range js.Jobs {
		job := &Job{}
		fill(j, job)
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (js *MockJobStorer) GetJob(id string) (*Job, error) {
	if j, found := js.Jobs[id]; found {
		job := &Job{}
		fill(j, job)
		return job, nil
	}
	return nil, nil
}

func (js *MockJobStorer) DeleteJob(id string) error {
	delete(js.Jobs, id)
	return nil
}

func fill(data string, out interface{}) {
	json.NewDecoder(bytes.NewBufferString(data)).Decode(out)
}
