package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/execute"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/orchestratorlog"
	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/ums"
	"github.comcast.com/mesa/mafslog/ecslog"
	"golang.org/x/exp/slog"
)

const numConsecutiveFailedStatesIsError = 5

type StageParams map[string]map[string]string

func (p StageParams) Merge(other StageParams) StageParams {
	params := StageParams{}
	for ns, nsVars := range p {
		if _, ok := params[ns]; !ok {
			params[ns] = map[string]string{}
		}
		for k, v := range nsVars {
			params[ns][k] = v
		}
	}
	for ns, nsVars := range other {
		if _, ok := params[ns]; !ok {
			params[ns] = map[string]string{}
		}
		for k, v := range nsVars {
			params[ns][k] = v
		}
	}
	return params
}

// Stage manages the execution of a single stage of a job, from start to finish.
type Stage struct {
	Namespace string
	JobID     string
	StageNum  int
	Outputs   []string
	Executor  execute.Executor
	tasks     []execute.Task
	logger    *ecslog.Logger
}

// Setup sets this stage up with a logger.
func (s *Stage) Setup(logger *ecslog.Logger) {
	s.logger = logger.With(s)
}

// ECSLogAttributes returns the log attributes of this type.
func (s *Stage) ECSLogAttributes() []slog.Attr {
	return orchestratorlog.MAFJob{StageNum: s.StageNum}.ECSLogAttributes()
}

// ID gets the unique identifier of this stage.
func (s *Stage) ID() string {
	return fmt.Sprintf("%s-s%d", s.JobID, s.StageNum)
}

// TaskID gets the unique identifier of the task at the given index in this stage.
func (s *Stage) TaskID(taskIdx int) string {
	return fmt.Sprintf("%s-t%d", s.ID(), taskIdx+1)
}

// Start starts the tasks that are part of this stage with the given parameters. In the case that the original stage was
// lost, start will not create any new resources, but will reconnect to the previously created tasks.
func (s *Stage) Start(ctx context.Context, parameters StageParams) error {
	if s.tasks == nil {
		s.tasks = []execute.Task{}
	}

	err := s.startTask(ctx, parameters)
	if err != nil {
		return err
	}

	for _, task := range s.tasks {
		err := execute.WaitUntilStarted(ctx, task, ResourcePollFrequency)
		if err != nil {
			return fmt.Errorf("task failed to start: %w", err)
		}
	}

	return nil
}

// startTask starts the only task that is currently possible to create with a stage.
func (s *Stage) startTask(ctx context.Context, parameters StageParams) error {
	taskID := s.TaskID(len(s.tasks))

	task, err := s.Executor.ReconnectTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to reconnect to task: %w", err)
	}
	if task != nil {
		s.logger.Info("Reconnected to task", task)
		s.tasks = append(s.tasks, task)
		return nil
	}

	s.logger.Info("Starting new task", task)

	template := s.Outputs[0] + ".yml"
	params := parameters.Merge(StageParams{
		"global": {
			"NamespaceID": "maf-analysis",
			"NodeType":    "test",
			"UniqueID":    s.ID(),
		},
	})

	task, err = s.Executor.Execute(ctx, taskID, template, params)
	if err != nil {
		return err
	}
	s.tasks = append(s.tasks, task)
	return nil
}

// ManageRunningLive manages the running stage in Live mode- this mode will check for errors as the job is running
// at the interval specified by job.resourcePollFrequency, and will return successfully when the end time that is
// returned by the getEndTimeFunc has passed.
func (s *Stage) ManageRunningLive(ctx context.Context, getEndTimeFunc func() ums.UnixMillis) error {
	var timer *time.Timer
	var prevEndTime ums.UnixMillis
	checkState := s.stateChecker(false)
	for {
		endTime := getEndTimeFunc()
		if prevEndTime == 0 {
			s.logger.Info(fmt.Sprintf("Running stage until %v", endTime.Time()))
		} else if endTime != prevEndTime {
			if endTime.Time().Before(time.Now()) {
				s.logger.Info(fmt.Sprintf("End time updated to %v which has already passed, stage complete", endTime.Time()))
				return nil
			}
			s.logger.Info(fmt.Sprintf("End time updated, now running stage until %v", endTime.Time()))
		}
		prevEndTime = endTime

		timeToWait := time.Until(endTime.Time())
		if timeToWait > ResourcePollFrequency {
			timeToWait = ResourcePollFrequency
		}
		timer = time.NewTimer(timeToWait)
		select {
		case <-timer.C:
			timer.Stop()
			if time.Since(getEndTimeFunc().Time()) >= 0 {
				return nil
			}

			if _, err := checkState(ctx); err != nil {
				return err
			}

			// TODO: check for non-kubernetes errors, i.e. no output or something like that

		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// ManageRunningFile manages the running stage in File mode- this mode will check for errors as the job is running
// at the interval specified by job.resourcePollFrequency, and will return successfully when the remote task's state is
// successfully completed.
func (s *Stage) ManageRunningFile(ctx context.Context) error {
	ticker := time.NewTicker(ResourcePollFrequency)
	checkState := s.stateChecker(true)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if done, err := checkState(ctx); err != nil {
				return err
			} else if done {
				return nil
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// stateChecker returns a stateCheck function. That function returns whether or not the job is complete, and reports any
// errors in the state of the tasks.
func (s *Stage) stateChecker(terminable bool) func(context.Context) (bool, error) {
	failedStates := 0
	return func(ctx context.Context) (bool, error) {
		tasksComplete := 0
		for _, t := range s.tasks {
			state, err := t.State(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return false, err
				}
				failedStates++
				if failedStates > numConsecutiveFailedStatesIsError {
					return false, fmt.Errorf("failed to check status of at least one task %d times: %w", numConsecutiveFailedStatesIsError, err)
				}
				return false, nil
			}

			switch state {
			case execute.TaskStateStarting, execute.TaskStateRunning:
				// good
			case execute.TaskStateComplete:
				if terminable {
					tasksComplete++
				} else {
					failedStates++
					if failedStates > numConsecutiveFailedStatesIsError {
						return false, fmt.Errorf("unexpected task state: %v", state)
					}
				}
			default:
				failedStates++
				if failedStates > numConsecutiveFailedStatesIsError {
					return false, fmt.Errorf("unexpected task state: %v", state)
				}
				return false, nil
			}
		}
		failedStates = 0
		if terminable {
			return tasksComplete == len(s.tasks), nil
		}
		return false, nil
	}
}

// Shutdown cleans up the resources that this stage manages.
func (s *Stage) Shutdown(ctx context.Context) error {
	errors := []error{}
	for _, task := range s.tasks {
		s.logger.Info("Killing task", task)
		err := task.Kill(ctx)
		if err != nil {
			errors = append(errors, err)
		}
	}
	s.tasks = []execute.Task{}

	if len(errors) > 0 {
		return fmt.Errorf("at least one task or instance failed: %v", errors)
	}
	return nil
}
