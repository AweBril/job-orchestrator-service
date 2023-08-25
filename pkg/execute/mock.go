package execute

import (
	"context"
	"fmt"
	"time"

	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/orchestratorlog"
	"golang.org/x/exp/slog"
)

type ExecutorResults struct {
	Duration time.Duration
	Task     *MockTask
	Error    error
}

type MockExecutor struct {
	ReconnectResults []ExecutorResults
	ExecuteResults   []ExecutorResults
}

func (e *MockExecutor) Execute(ctx context.Context, id, templateName string, parameters any) (Task, error) {
	if len(e.ExecuteResults) == 0 {
		return nil, fmt.Errorf("no execute results left")
	}
	nextResults := e.ExecuteResults[0]
	e.ExecuteResults = e.ExecuteResults[1:]
	timer := time.NewTimer(nextResults.Duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if nextResults.Task != nil {
		if nextResults.Task.ID == "" {
			nextResults.Task.ID = id
		}
		if nextResults.Task.StartTime.IsZero() {
			nextResults.Task.StartTime = time.Now()
		}
	}

	return nextResults.Task, nextResults.Error
}

func (e *MockExecutor) ReconnectTask(ctx context.Context, id string) (Task, error) {
	if len(e.ReconnectResults) == 0 {
		return nil, fmt.Errorf("no reconnect results left")
	}
	nextResults := e.ReconnectResults[0]
	e.ReconnectResults = e.ReconnectResults[1:]
	timer := time.NewTimer(nextResults.Duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if nextResults.Task == nil {
		return nil, nextResults.Error
	}

	if nextResults.Task.ID == "" {
		nextResults.Task.ID = id
	}
	if nextResults.Task.StartTime.IsZero() {
		nextResults.Task.StartTime = time.Now()
	}

	return nextResults.Task, nextResults.Error
}

type MockTaskState struct {
	Duration     time.Duration
	State        TaskState
	FailedReason string
}

type MockTaskResults struct {
	Duration time.Duration
	Error    error
}

type MockTask struct {
	ID           string
	StartTime    time.Time
	States       []MockTaskState
	KillResults  []MockTaskResults
	failedReason string
}

func (t *MockTask) GetID() string {
	return t.ID
}

func (t *MockTask) State(context.Context) (TaskState, error) {
	endTime := t.StartTime
	for _, state := range t.States {
		endTime = endTime.Add(state.Duration)
		if time.Now().Before(endTime) {
			t.failedReason = state.FailedReason
			return state.State, nil
		}
	}
	return TaskStateUnknown, fmt.Errorf("time not in state time range, could be intentional")
}

func (t *MockTask) Kill(ctx context.Context) error {
	if len(t.KillResults) == 0 {
		return nil
	}
	nextResults := t.KillResults[0]
	t.KillResults = t.KillResults[1:]
	timer := time.NewTimer(nextResults.Duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nextResults.Error
}

func (t *MockTask) FailedReason() string {
	return t.failedReason
}

func (t *MockTask) ECSLogAttributes() []slog.Attr {
	return orchestratorlog.MAFJob{TaskID: t.GetID()}.ECSLogAttributes()
}
