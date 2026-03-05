package pipex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHooksRunStartRunEndSuccess(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	var (
		startCount atomic.Int64
		endCount   atomic.Int64
		startRunID string
		endRunID   string
		endErr     error
		mu         sync.Mutex
	)

	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			startCount.Add(1)
			mu.Lock()
			startRunID = meta.RunID
			mu.Unlock()
		},
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			endCount.Add(1)
			mu.Lock()
			endRunID = meta.RunID
			endErr = err
			mu.Unlock()
		},
	}

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}}, WithHooks[int](hooks))
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if got := startCount.Load(); got != 1 {
		t.Fatalf("expected RunStart once, got %d", got)
	}
	if got := endCount.Load(); got != 1 {
		t.Fatalf("expected RunEnd once, got %d", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if startRunID == "" || endRunID == "" {
		t.Fatalf("expected non-empty run ids, got start=%q end=%q", startRunID, endRunID)
	}
	if startRunID != endRunID {
		t.Fatalf("run id mismatch: start=%q end=%q", startRunID, endRunID)
	}
	if endErr != nil {
		t.Fatalf("expected nil RunEnd error, got %v", endErr)
	}
}

func TestHooksRunEndCalledOnceOnError(t *testing.T) {
	p := NewPipeline[int]()
	wantErr := errors.New("boom")
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return nil, wantErr
		},
	})

	var (
		endCount atomic.Int64
		endErr   error
		mu       sync.Mutex
	)
	hooks := Hooks[int]{
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			endCount.Add(1)
			mu.Lock()
			endErr = err
			mu.Unlock()
		},
	}

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}}, WithHooks[int](hooks))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected run error to include %v, got %v", wantErr, err)
	}
	if got := endCount.Load(); got != 1 {
		t.Fatalf("expected RunEnd once, got %d", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(endErr, wantErr) {
		t.Fatalf("expected RunEnd error to include %v, got %v", wantErr, endErr)
	}
}

func TestHooksStageEventOrder(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p := NewPipeline[int]()
		_ = p.AddStage(testStage[int]{name: "a", workers: 1})

		var (
			events []string
			mu     sync.Mutex
		)
		hooks := Hooks[int]{
			StageStart: func(ctx context.Context, e StageStartEvent[int]) {
				mu.Lock()
				events = append(events, "start")
				mu.Unlock()
			},
			StageFinish: func(ctx context.Context, e StageFinishEvent[int]) {
				mu.Lock()
				events = append(events, "finish")
				mu.Unlock()
			},
			StageError: func(ctx context.Context, e StageErrorEvent[int]) {
				mu.Lock()
				events = append(events, "error")
				mu.Unlock()
			},
		}

		_, err := p.Run(context.Background(), map[string][]int{"a": {1}}, WithHooks[int](hooks))
		if err != nil {
			t.Fatalf("unexpected run error: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(events) != 2 || events[0] != "start" || events[1] != "finish" {
			t.Fatalf("unexpected stage event order: %v", events)
		}
	})

	t.Run("error", func(t *testing.T) {
		p := NewPipeline[int]()
		wantErr := errors.New("stage failed")
		_ = p.AddStage(testStage[int]{
			name:    "a",
			workers: 1,
			fn: func(ctx context.Context, in int) ([]int, error) {
				return nil, wantErr
			},
		})

		var (
			events []string
			mu     sync.Mutex
		)
		hooks := Hooks[int]{
			StageStart: func(ctx context.Context, e StageStartEvent[int]) {
				mu.Lock()
				events = append(events, "start")
				mu.Unlock()
			},
			StageFinish: func(ctx context.Context, e StageFinishEvent[int]) {
				mu.Lock()
				events = append(events, "finish")
				mu.Unlock()
			},
			StageError: func(ctx context.Context, e StageErrorEvent[int]) {
				mu.Lock()
				events = append(events, "error")
				mu.Unlock()
			},
		}

		_, err := p.Run(context.Background(), map[string][]int{"a": {1}}, WithHooks[int](hooks))
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected run error to include %v, got %v", wantErr, err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(events) != 2 || events[0] != "start" || events[1] != "error" {
			t.Fatalf("unexpected stage event order: %v", events)
		}
	})
}

func TestHooksStagePanicRecovered(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	var finishCount atomic.Int64
	hooks := Hooks[int]{
		StageStart: func(ctx context.Context, e StageStartEvent[int]) {
			panic("hook panic")
		},
		StageFinish: func(ctx context.Context, e StageFinishEvent[int]) {
			finishCount.Add(1)
		},
	}

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}}, WithHooks[int](hooks))
	if err != nil {
		t.Fatalf("unexpected run error with panicking hook: %v", err)
	}
	if got := finishCount.Load(); got != 1 {
		t.Fatalf("expected StageFinish once, got %d", got)
	}
}

func TestHooksSinkRetryAndExhausted(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	sink := testSink[int]{
		name:  "s",
		stage: "a",
		fn: func(ctx context.Context, item int) error {
			return errors.New("sink down")
		},
	}

	var (
		retryCount     atomic.Int64
		exhaustedCount atomic.Int64
		retryAttempt   int
		exhaustAttempt int
		mu             sync.Mutex
	)
	hooks := Hooks[int]{
		SinkRetry: func(ctx context.Context, e SinkRetryEvent[int]) {
			retryCount.Add(1)
			mu.Lock()
			retryAttempt = e.Attempt
			mu.Unlock()
		},
		SinkExhausted: func(ctx context.Context, e SinkExhaustedEvent[int]) {
			exhaustedCount.Add(1)
			mu.Lock()
			exhaustAttempt = e.Attempts
			mu.Unlock()
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithSinks[int](sink),
		WithSinkRetry[int](1, time.Millisecond),
		WithHooks[int](hooks),
	)
	if err == nil {
		t.Fatal("expected sink exhaustion error")
	}

	if got := retryCount.Load(); got != 1 {
		t.Fatalf("expected one retry event, got %d", got)
	}
	if got := exhaustedCount.Load(); got != 1 {
		t.Fatalf("expected one exhausted event, got %d", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if retryAttempt != 1 {
		t.Fatalf("unexpected retry attempt: got %d want %d", retryAttempt, 1)
	}
	if exhaustAttempt != 2 {
		t.Fatalf("unexpected exhausted attempts: got %d want %d", exhaustAttempt, 2)
	}
}

func TestHooksTriggerErrorWithoutTriggerEnd(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	wantErr := errors.New("trigger failed")
	tr := testTrigger[int]{
		name:  "t1",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			return wantErr
		},
	}

	var (
		startCount atomic.Int64
		errCount   atomic.Int64
		endCount   atomic.Int64
	)
	hooks := Hooks[int]{
		TriggerStart: func(ctx context.Context, e TriggerStartEvent[int]) {
			startCount.Add(1)
		},
		TriggerError: func(ctx context.Context, e TriggerErrorEvent[int]) {
			errCount.Add(1)
		},
		TriggerEnd: func(ctx context.Context, e TriggerEndEvent[int]) {
			endCount.Add(1)
		},
	}

	_, err := p.Run(context.Background(), nil, WithTriggers[int](tr), WithHooks[int](hooks))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected run error to include %v, got %v", wantErr, err)
	}
	if got := startCount.Load(); got != 1 {
		t.Fatalf("expected TriggerStart once, got %d", got)
	}
	if got := errCount.Load(); got != 1 {
		t.Fatalf("expected TriggerError once, got %d", got)
	}
	if got := endCount.Load(); got != 0 {
		t.Fatalf("expected no TriggerEnd on trigger error, got %d", got)
	}
}
