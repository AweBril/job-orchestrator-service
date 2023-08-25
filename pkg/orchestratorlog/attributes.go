package orchestratorlog

import (
	"github.comcast.com/mesa/mafslog/logutil"
	"golang.org/x/exp/slog"
)

const MAFJobLogKey = "maf_job"

// MAFJob has all the fields that would help to point a specific log message to a job, and what is running within that job.
type MAFJob struct {
	Namespace string `json:"namespace,omitempty"`
	JobID     string `json:"jobId,omitempty"`
	StageNum  int    `json:"stageNum,omitempty"`
	TaskID    string `json:"taskId,omitempty"`
}

// ECSLogAttributes returns the log attributes of this type.
func (j MAFJob) ECSLogAttributes() []slog.Attr {
	return []slog.Attr{logutil.Any(MAFJobLogKey, j)}
}
