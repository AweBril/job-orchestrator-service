package execute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.comcast.com/mesa/mafslog/ecslog/ecsattr"
)

// Executor is a component that executes new Tasks based on the parameters given.
type Executor interface {
	Execute(ctx context.Context, id, templateName string, parameters any) (Task, error)
	ReconnectTask(ctx context.Context, id string) (Task, error)
}

type TaskState string

const (
	TaskStateStarting TaskState = "starting"
	TaskStateRunning  TaskState = "running"
	TaskStateComplete TaskState = "complete"
	TaskStateFailed   TaskState = "failed"
	TaskStateUnknown  TaskState = ""
)

// Task wraps an external executable resource as a logical unit of work within MAF.
type Task interface {
	GetID() string
	State(context.Context) (TaskState, error)
	Kill(context.Context) error
	FailedReason() string

	ecsattr.ECSLogAttributer
}

// WaitUntilStarted polls this task at the given frequency until its state says it is running, cancellable by the given context.
func WaitUntilStarted(ctx context.Context, task Task, pollFrequency time.Duration) error {
	ticker := time.NewTicker(pollFrequency)
	defer ticker.Stop()
	for {
		state, err := task.State(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("task still starting: %w", err)
			} else {
				return fmt.Errorf("failed getting task state: %w", err)
			}
		}

		switch state {
		case TaskStateStarting:
			// exit switch
		case TaskStateRunning, TaskStateComplete:
			return nil
		case TaskStateFailed:
			return fmt.Errorf("task in failed state, with reason: %s", task.FailedReason())
		default:
			return fmt.Errorf("task in unknown state %s", state)
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return fmt.Errorf("task still starting: %w", ctx.Err())
		}
	}
}
