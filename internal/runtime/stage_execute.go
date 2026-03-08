package runtime

import (
	"context"
	"errors"
	"time"
)

type StageExecutionPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	Timeout     time.Duration
}

type StageExecutionResult[T any] struct {
	Out        []T
	Err        error
	AckOnError bool
}

type StageExecutionConfig[T any] struct {
	Ctx          context.Context
	Stage        string
	Input        T
	Policy       StageExecutionPolicy
	Process      func(context.Context, T) ([]T, error)
	IsContextErr func(error) bool
	WaitRate     func(context.Context) error

	OnStageStart   func(startedAt time.Time)
	OnStageFinish  func(outCount int, startedAt, finishedAt time.Time)
	OnStageError   func(startedAt, finishedAt time.Time, err error)
	OnAttemptStart func(attempt int, startedAt time.Time)
	OnAttemptError func(attempt int, startedAt, finishedAt time.Time, err error)
	OnRetry        func(attempt int, err error, backoff time.Duration, at time.Time)
	OnTimeout      func(attempt int, startedAt time.Time, timeout time.Duration, at time.Time)
	OnExhausted    func(attempt int, err error, at time.Time)
}

func ExecuteStageWithPolicy[T any](cfg StageExecutionConfig[T]) StageExecutionResult[T] {
	if cfg.WaitRate != nil {
		if err := cfg.WaitRate(cfg.Ctx); err != nil {
			return StageExecutionResult[T]{
				Err:        err,
				AckOnError: false,
			}
		}
	}

	policy := cfg.Policy
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}

	stageStart := time.Now()
	if cfg.OnStageStart != nil {
		cfg.OnStageStart(stageStart)
	}

	var (
		out []T
		err error
	)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		attemptCtx := cfg.Ctx
		cancel := func() {}
		if policy.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(cfg.Ctx, policy.Timeout)
		}

		attemptStart := time.Now()
		if cfg.OnAttemptStart != nil {
			cfg.OnAttemptStart(attempt, attemptStart)
		}
		out, err = cfg.Process(attemptCtx, cfg.Input)
		attemptFinish := time.Now()
		cancel()

		if err == nil {
			if cfg.OnStageFinish != nil {
				cfg.OnStageFinish(len(out), stageStart, attemptFinish)
			}
			return StageExecutionResult[T]{
				Out:        out,
				Err:        nil,
				AckOnError: false,
			}
		}

		if cfg.OnAttemptError != nil {
			cfg.OnAttemptError(attempt, attemptStart, attemptFinish, err)
		}
		if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) && cfg.OnTimeout != nil {
			cfg.OnTimeout(attempt, attemptStart, policy.Timeout, attemptFinish)
		}

		// stop immediately on run cancellation
		if cfg.Ctx.Err() != nil {
			if cfg.IsContextErr != nil && !cfg.IsContextErr(err) {
				if cfg.OnStageError != nil {
					cfg.OnStageError(stageStart, attemptFinish, err)
				}
				return StageExecutionResult[T]{
					Err:        err,
					AckOnError: false,
				}
			}
			return StageExecutionResult[T]{
				Err:        cfg.Ctx.Err(),
				AckOnError: false,
			}
		}

		// no retries left
		if attempt == policy.MaxAttempts {
			if cfg.OnExhausted != nil {
				cfg.OnExhausted(attempt, err, attemptFinish)
			}
			if cfg.OnStageError != nil {
				cfg.OnStageError(stageStart, attemptFinish, err)
			}
			return StageExecutionResult[T]{
				Err:        err,
				AckOnError: true,
			}
		}

		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt, err, policy.Backoff, attemptFinish)
		}
		if policy.Backoff > 0 {
			select {
			case <-cfg.Ctx.Done():
				return StageExecutionResult[T]{
					Err:        cfg.Ctx.Err(),
					AckOnError: false,
				}
			case <-time.After(policy.Backoff):
			}
		}
	}

	return StageExecutionResult[T]{}
}
