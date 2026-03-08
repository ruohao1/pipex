package pipex

import (
	"context"
	"errors"
	"strconv"
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

func TestHooksRunEndGetsExactReturnedErrorForMissingTriggerStage(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	tr := testTrigger[int]{
		name:  "bad",
		stage: "missing",
	}

	var runEndErr error
	hooks := Hooks[int]{
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			runEndErr = err
		},
	}

	_, err := p.Run(context.Background(), nil, WithTriggers[int](tr), WithHooks[int](hooks))
	if err == nil {
		t.Fatal("expected missing trigger stage error")
	}
	if runEndErr == nil {
		t.Fatal("expected RunEnd error to be set")
	}
	if err.Error() != runEndErr.Error() {
		t.Fatalf("expected RunEnd error to match returned error: run=%q return=%q", runEndErr.Error(), err.Error())
	}
}

func TestHooksRunEndGetsExactReturnedErrorForMissingSinkStage(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	sink := testSink[int]{
		name:  "bad-sink",
		stage: "missing",
	}

	var runEndErr error
	hooks := Hooks[int]{
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			runEndErr = err
		},
	}

	_, err := p.Run(context.Background(), nil, WithSinks[int](sink), WithHooks[int](hooks))
	if err == nil {
		t.Fatal("expected missing sink stage error")
	}
	if runEndErr == nil {
		t.Fatal("expected RunEnd error to be set")
	}
	if err.Error() != runEndErr.Error() {
		t.Fatalf("expected RunEnd error to match returned error: run=%q return=%q", runEndErr.Error(), err.Error())
	}
}

func TestHooksRunMetaEdgeCountIsTotalEdges(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.AddStage(testStage[int]{name: "c", workers: 1})
	_ = p.AddStage(testStage[int]{name: "d", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("a", "c")
	_ = p.Connect("b", "d")

	var gotMeta RunMeta
	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			gotMeta = meta
		},
	}

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}}, WithHooks[int](hooks))
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if gotMeta.EdgeCount != 3 {
		t.Fatalf("unexpected EdgeCount: got %d want %d", gotMeta.EdgeCount, 3)
	}
}

func TestHooksRunMetaUnchangedWithStageWorkersOverride(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")

	var gotMeta RunMeta
	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			gotMeta = meta
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageWorkers[int](map[string]int{"a": 4, "b": 2}),
		WithHooks[int](hooks),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if gotMeta.StageCount != 2 {
		t.Fatalf("unexpected StageCount: got %d want %d", gotMeta.StageCount, 2)
	}
	if gotMeta.EdgeCount != 1 {
		t.Fatalf("unexpected EdgeCount: got %d want %d", gotMeta.EdgeCount, 1)
	}
	if gotMeta.SeedStages != 1 || gotMeta.SeedItems != 1 {
		t.Fatalf("unexpected seed metadata: SeedStages=%d SeedItems=%d", gotMeta.SeedStages, gotMeta.SeedItems)
	}
}

func TestHooksRunEndGetsExactReturnedErrorForUnknownStageWorkersOverride(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	var runEndErr error
	hooks := Hooks[int]{
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			runEndErr = err
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageWorkers[int](map[string]int{"missing": 2}),
		WithHooks[int](hooks),
	)
	if err == nil {
		t.Fatal("expected stage-worker config error")
	}
	if runEndErr == nil {
		t.Fatal("expected RunEnd error to be set")
	}
	if err.Error() != runEndErr.Error() {
		t.Fatalf("expected RunEnd error to match returned error: run=%q return=%q", runEndErr.Error(), err.Error())
	}
}

func TestHooksRunEndGetsExactReturnedErrorForBadStageRateLimitsConfig(t *testing.T) {
	t.Run("unknown stage", func(t *testing.T) {
		p := NewPipeline[int]()
		_ = p.AddStage(testStage[int]{name: "a", workers: 1})

		var runEndErr error
		hooks := Hooks[int]{
			RunEnd: func(ctx context.Context, meta RunMeta, err error) {
				runEndErr = err
			},
		}

		_, err := p.Run(
			context.Background(),
			map[string][]int{"a": {1}},
			WithStageRateLimits[int](map[string]RateLimit{"missing": {RPS: 10, Burst: 1}}),
			WithHooks[int](hooks),
		)
		if err == nil {
			t.Fatal("expected stage-rate-limits config error")
		}
		if runEndErr == nil {
			t.Fatal("expected RunEnd error to be set")
		}
		if err.Error() != runEndErr.Error() {
			t.Fatalf("expected RunEnd error to match returned error: run=%q return=%q", runEndErr.Error(), err.Error())
		}
	})

	t.Run("invalid rps", func(t *testing.T) {
		p := NewPipeline[int]()
		_ = p.AddStage(testStage[int]{name: "a", workers: 1})

		var runEndErr error
		hooks := Hooks[int]{
			RunEnd: func(ctx context.Context, meta RunMeta, err error) {
				runEndErr = err
			},
		}

		_, err := p.Run(
			context.Background(),
			map[string][]int{"a": {1}},
			WithStageRateLimits[int](map[string]RateLimit{"a": {RPS: 0, Burst: 1}}),
			WithHooks[int](hooks),
		)
		if err == nil {
			t.Fatal("expected stage-rate-limits config error")
		}
		if runEndErr == nil {
			t.Fatal("expected RunEnd error to be set")
		}
		if err.Error() != runEndErr.Error() {
			t.Fatalf("expected RunEnd error to match returned error: run=%q return=%q", runEndErr.Error(), err.Error())
		}
	})
}

func TestHooksDedupDropPayload(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	var (
		dropCount atomic.Int64
		dropEvt   DedupDropEvent[int]
	)
	hooks := Hooks[int]{
		DedupDrop: func(ctx context.Context, e DedupDropEvent[int]) {
			dropCount.Add(1)
			dropEvt = e
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 1}},
		WithDedupRules[int](DedupRule[int]{
			Name:  "global-dedup",
			Scope: DedupScopeGlobal,
			Key: func(v int) string {
				return "k"
			},
		}),
		WithHooks[int](hooks),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := dropCount.Load(); got != 1 {
		t.Fatalf("expected one DedupDrop event, got %d", got)
	}
	if dropEvt.Scope != DedupScopeGlobal {
		t.Fatalf("unexpected dedup scope: got %q want %q", dropEvt.Scope, DedupScopeGlobal)
	}
	if dropEvt.Key != "a\x00k" {
		t.Fatalf("unexpected dedup key: got %q want %q", dropEvt.Key, "a\x00k")
	}
}

func TestHooksStageAttemptRetrySequence(t *testing.T) {
	p := NewPipeline[int]()
	var attempts atomic.Int64
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("transient")
			}
			return []int{in}, nil
		},
	})

	var (
		mu     sync.Mutex
		events []string
	)
	hooks := Hooks[int]{
		StageStart: func(ctx context.Context, e StageStartEvent[int]) {
			mu.Lock()
			events = append(events, "stage_start")
			mu.Unlock()
		},
		StageAttemptStart: func(ctx context.Context, e StageAttemptStartEvent[int]) {
			mu.Lock()
			events = append(events, "attempt_start_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageAttemptError: func(ctx context.Context, e StageAttemptErrorEvent[int]) {
			mu.Lock()
			events = append(events, "attempt_error_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageRetry: func(ctx context.Context, e StageRetryEvent[int]) {
			mu.Lock()
			events = append(events, "retry_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageFinish: func(ctx context.Context, e StageFinishEvent[int]) {
			mu.Lock()
			events = append(events, "stage_finish")
			mu.Unlock()
		},
		StageTimeout: func(ctx context.Context, e StageTimeoutEvent[int]) {
			mu.Lock()
			events = append(events, "timeout")
			mu.Unlock()
		},
		StageExhausted: func(ctx context.Context, e StageExhaustedEvent[int]) {
			mu.Lock()
			events = append(events, "exhausted")
			mu.Unlock()
		},
		StageError: func(ctx context.Context, e StageErrorEvent[int]) {
			mu.Lock()
			events = append(events, "stage_error")
			mu.Unlock()
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStagePolicies[int](map[string]StagePolicy{
			"a": {MaxAttempts: 2},
		}),
		WithHooks[int](hooks),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	want := []string{
		"stage_start",
		"attempt_start_1",
		"attempt_error_1",
		"retry_1",
		"attempt_start_2",
		"stage_finish",
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != len(want) {
		t.Fatalf("unexpected events len: got %d want %d events=%v", len(events), len(want), events)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("unexpected event at %d: got %q want %q all=%v", i, events[i], want[i], events)
		}
	}
}

func TestHooksStageAttemptTimeoutAndExhaustedSequence(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	var (
		mu     sync.Mutex
		events []string
	)
	hooks := Hooks[int]{
		StageStart: func(ctx context.Context, e StageStartEvent[int]) {
			mu.Lock()
			events = append(events, "stage_start")
			mu.Unlock()
		},
		StageAttemptStart: func(ctx context.Context, e StageAttemptStartEvent[int]) {
			mu.Lock()
			events = append(events, "attempt_start_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageAttemptError: func(ctx context.Context, e StageAttemptErrorEvent[int]) {
			mu.Lock()
			events = append(events, "attempt_error_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageTimeout: func(ctx context.Context, e StageTimeoutEvent[int]) {
			mu.Lock()
			events = append(events, "timeout_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageRetry: func(ctx context.Context, e StageRetryEvent[int]) {
			mu.Lock()
			events = append(events, "retry_"+strconv.Itoa(e.Attempt))
			mu.Unlock()
		},
		StageExhausted: func(ctx context.Context, e StageExhaustedEvent[int]) {
			mu.Lock()
			events = append(events, "exhausted")
			mu.Unlock()
		},
		StageError: func(ctx context.Context, e StageErrorEvent[int]) {
			mu.Lock()
			events = append(events, "stage_error")
			mu.Unlock()
		},
		StageFinish: func(ctx context.Context, e StageFinishEvent[int]) {
			mu.Lock()
			events = append(events, "stage_finish")
			mu.Unlock()
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStagePolicies[int](map[string]StagePolicy{
			"a": {MaxAttempts: 2, Timeout: 10 * time.Millisecond},
		}),
		WithHooks[int](hooks),
	)
	// Run currently suppresses context-derived errors from worker jobs in
	// non-fail-fast mode; this test focuses on hook sequencing guarantees.
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	want := []string{
		"stage_start",
		"attempt_start_1",
		"attempt_error_1",
		"timeout_1",
		"retry_1",
		"attempt_start_2",
		"attempt_error_2",
		"timeout_2",
		"exhausted",
		"stage_error",
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != len(want) {
		t.Fatalf("unexpected events len: got %d want %d events=%v", len(events), len(want), events)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("unexpected event at %d: got %q want %q all=%v", i, events[i], want[i], events)
		}
	}
}

func TestHooksConcurrencyUnderLoad(t *testing.T) {
	const n = 1000

	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	})
	_ = p.Connect("a", "b")

	seeds := make([]int, n)
	for i := range n {
		seeds[i] = i
	}

	var (
		stageStartCount  atomic.Int64
		stageFinishCount atomic.Int64
		runStartCount    atomic.Int64
		runEndCount      atomic.Int64
	)
	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			runStartCount.Add(1)
		},
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			runEndCount.Add(1)
		},
		StageStart: func(ctx context.Context, e StageStartEvent[int]) {
			stageStartCount.Add(1)
		},
		StageFinish: func(ctx context.Context, e StageFinishEvent[int]) {
			stageFinishCount.Add(1)
		},
	}

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": seeds},
		WithHooks[int](hooks),
		WithBufferSize[int](32),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != n {
		t.Fatalf("unexpected stage a count: got %d want %d", got, n)
	}
	if got := len(res["b"]); got != n {
		t.Fatalf("unexpected stage b count: got %d want %d", got, n)
	}

	if got := runStartCount.Load(); got != 1 {
		t.Fatalf("expected RunStart once, got %d", got)
	}
	if got := runEndCount.Load(); got != 1 {
		t.Fatalf("expected RunEnd once, got %d", got)
	}
	if got := stageStartCount.Load(); got != int64(2*n) {
		t.Fatalf("unexpected StageStart count: got %d want %d", got, 2*n)
	}
	if got := stageFinishCount.Load(); got != int64(2*n) {
		t.Fatalf("unexpected StageFinish count: got %d want %d", got, 2*n)
	}
}

func TestFrontierHooksEmitted(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")

	var (
		enqueueCount atomic.Int64
		reserveCount atomic.Int64
		ackCount     atomic.Int64
		retryCount   atomic.Int64
	)
	hooks := Hooks[int]{
		FrontierEnqueue: func(ctx context.Context, e FrontierEnqueueEvent[int]) {
			enqueueCount.Add(1)
		},
		FrontierReserve: func(ctx context.Context, e FrontierReserveEvent[int]) {
			reserveCount.Add(1)
		},
		FrontierAck: func(ctx context.Context, e FrontierAckEvent[int]) {
			ackCount.Add(1)
		},
		FrontierRetry: func(ctx context.Context, e FrontierRetryEvent[int]) {
			retryCount.Add(1)
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithFrontier[int](true),
		WithHooks[int](hooks),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if got := enqueueCount.Load(); got == 0 {
		t.Fatal("expected frontier enqueue hooks")
	}
	if got := reserveCount.Load(); got == 0 {
		t.Fatal("expected frontier reserve hooks")
	}
	if got := ackCount.Load(); got == 0 {
		t.Fatal("expected frontier ack hooks")
	}
	if got := retryCount.Load(); got != 0 {
		t.Fatalf("expected no frontier retry hook on success path, got %d", got)
	}
}
