package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecuteStageWithPolicy_ExhaustedSetsAckOnError(t *testing.T) {
	cfg := StageExecutionConfig[int]{
		Ctx:   context.Background(),
		Stage: "s",
		Input: 1,
		Policy: StageExecutionPolicy{
			MaxAttempts: 2,
			Backoff:     0,
		},
		Process: func(ctx context.Context, in int) ([]int, error) {
			return nil, errors.New("boom")
		},
		IsContextErr: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		},
	}

	res := ExecuteStageWithPolicy(cfg)
	if res.Err == nil {
		t.Fatal("expected error, got nil")
	}
	if !res.AckOnError {
		t.Fatal("expected AckOnError=true")
	}
}

func TestExecuteStageWithPolicy_ContextCanceledNoAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := StageExecutionConfig[int]{
		Ctx:   ctx,
		Stage: "s",
		Input: 1,
		Policy: StageExecutionPolicy{
			MaxAttempts: 1,
		},
		Process: func(ctx context.Context, in int) ([]int, error) {
			return nil, context.Canceled
		},
		IsContextErr: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		},
	}

	res := ExecuteStageWithPolicy(cfg)
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", res.Err)
	}
	if res.AckOnError {
		t.Fatal("expected AckOnError=false")
	}
}

func TestExecuteStageWithPolicy_RetryThenSuccess(t *testing.T) {
	attempts := 0
	cfg := StageExecutionConfig[int]{
		Ctx:   context.Background(),
		Stage: "s",
		Input: 2,
		Policy: StageExecutionPolicy{
			MaxAttempts: 3,
			Backoff:     1 * time.Millisecond,
		},
		Process: func(ctx context.Context, in int) ([]int, error) {
			attempts++
			if attempts < 2 {
				return nil, errors.New("transient")
			}
			return []int{in + 1}, nil
		},
		IsContextErr: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		},
	}

	res := ExecuteStageWithPolicy(cfg)
	if res.Err != nil {
		t.Fatalf("unexpected err: %v", res.Err)
	}
	if res.AckOnError {
		t.Fatal("expected AckOnError=false")
	}
	if len(res.Out) != 1 || res.Out[0] != 3 {
		t.Fatalf("unexpected out: %#v", res.Out)
	}
}
