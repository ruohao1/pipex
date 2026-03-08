package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestEnqueueDirect_StageNotFound(t *testing.T) {
	want := errors.New("missing")
	err := EnqueueDirect(DirectEnqueueConfig[int]{
		Ctx:       context.Background(),
		StageName: "a",
		LookupQueue: func(stage string) (chan int, bool) {
			return nil, false
		},
		BuildStageNotFound: func(stage string) error { return want },
		BuildJob: func(stage string) int {
			return 1
		},
	})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
}

func TestEnqueueDirect_CancelAfterBuild(t *testing.T) {
	q := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())
	var canceled atomic.Int64
	var scheduled atomic.Int64
	errCh := make(chan error, 1)

	go func() {
		errCh <- EnqueueDirect(DirectEnqueueConfig[int]{
			Ctx:       ctx,
			StageName: "a",
			LookupQueue: func(stage string) (chan int, bool) {
				return q, true
			},
			BuildStageNotFound: func(stage string) error { return errors.New("missing") },
			BeforeSchedule: func() {
				scheduled.Add(1)
				cancel()
			},
			OnCanceledAfterBuild: func() {
				canceled.Add(1)
			},
			BuildJob: func(stage string) int { return 1 },
		})
	}()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if scheduled.Load() != 1 {
		t.Fatalf("scheduled=%d want=1", scheduled.Load())
	}
	if canceled.Load() != 1 {
		t.Fatalf("canceled=%d want=1", canceled.Load())
	}
}

func TestEnqueueDirect_Success(t *testing.T) {
	q := make(chan int, 1)
	err := EnqueueDirect(DirectEnqueueConfig[int]{
		Ctx:       context.Background(),
		StageName: "a",
		LookupQueue: func(stage string) (chan int, bool) {
			return q, true
		},
		BuildStageNotFound: func(stage string) error { return errors.New("missing") },
		BuildJob:           func(stage string) int { return 7 },
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	got := <-q
	if got != 7 {
		t.Fatalf("got=%d want=7", got)
	}
}
