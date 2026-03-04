package pipex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testStage[T any] struct {
	name    string
	workers int
	fn      func(context.Context, T) ([]T, error)
}

func (s testStage[T]) Name() string { return s.name }

func (s testStage[T]) Workers() int { return s.workers }

func (s testStage[T]) Process(ctx context.Context, in T) (out []T, err error) {
	if s.fn != nil {
		return s.fn(ctx, in)
	}
	return []T{in}, nil
}

type testTrigger[T any] struct {
	name  string
	stage string
	fn    func(context.Context, func(T) error) error
}

func (t testTrigger[T]) Name() string { return t.name }

func (t testTrigger[T]) Stage() string { return t.stage }

func (t testTrigger[T]) Start(ctx context.Context, emit func(T) error) error {
	if t.fn != nil {
		return t.fn(ctx, emit)
	}
	return nil
}

type testSink[T any] struct {
	name  string
	stage string
	fn    func(context.Context, T) error
}

func (s testSink[T]) Name() string { return s.name }

func (s testSink[T]) Stage() string { return s.stage }

func (s testSink[T]) Consume(ctx context.Context, item T) error {
	if s.fn != nil {
		return s.fn(ctx, item)
	}
	return nil
}

func TestAddStage(t *testing.T) {
	p := NewPipeline[int]()

	if err := p.AddStage(nil); err != ErrNilStage {
		t.Fatalf("expected ErrNilStage, got %v", err)
	}

	stage := testStage[int]{name: "a", workers: 1}
	if err := p.AddStage(stage); err != nil {
		t.Fatalf("unexpected add stage error: %v", err)
	}

	if err := p.AddStage(stage); err == nil {
		t.Fatalf("expected duplicate stage error")
	}
}

func TestConnectMissingStage(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	if err := p.Connect("a", "missing"); err == nil {
		t.Fatalf("expected missing target stage error")
	} else if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound, got %v", err)
	}

	if err := p.Connect("missing", "a"); err == nil {
		t.Fatalf("expected missing source stage error")
	} else if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound, got %v", err)
	}
}

func TestConnectSelfCycle(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	if err := p.Connect("a", "a"); err != ErrCycle {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
}

func TestValidateNoStages(t *testing.T) {
	p := NewPipeline[int]()
	if err := p.Validate(); err != ErrNoStages {
		t.Fatalf("expected ErrNoStages, got %v", err)
	}
}

func TestValidateUnknownStageReferencedByEdge(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	p.edges["a"] = append(p.edges["a"], "ghost")
	if err := p.Validate(); err == nil {
		t.Fatalf("expected stage-not-found error")
	}
}

func TestValidateDetectsCycle(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.AddStage(testStage[int]{name: "c", workers: 1})

	if err := p.Connect("a", "b"); err != nil {
		t.Fatalf("unexpected connect error: %v", err)
	}
	if err := p.Connect("b", "c"); err != nil {
		t.Fatalf("unexpected connect error: %v", err)
	}
	if err := p.Connect("c", "a"); err != nil {
		t.Fatalf("unexpected connect error: %v", err)
	}

	if err := p.Validate(); err != ErrCycle {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
}

func TestValidateAcyclicGraph(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.AddStage(testStage[int]{name: "c", workers: 1})

	if err := p.Connect("a", "b"); err != nil {
		t.Fatalf("unexpected connect error: %v", err)
	}
	if err := p.Connect("b", "c"); err != nil {
		t.Fatalf("unexpected connect error: %v", err)
	}

	if err := p.Validate(); err != nil {
		t.Fatalf("expected valid graph, got %v", err)
	}
}

func TestRunLinearFlow(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	})
	_ = p.Connect("a", "b")

	res, err := p.Run(context.Background(), map[string][]int{"a": {1}})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := res["a"]; len(got) != 1 || got[0] != 2 {
		t.Fatalf("unexpected stage a outputs: %v", got)
	}
	if got := res["b"]; len(got) != 1 || got[0] != 4 {
		t.Fatalf("unexpected stage b outputs: %v", got)
	}
}

func TestRunFanOut(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 10}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "c",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 100}, nil
		},
	})
	_ = p.Connect("a", "b")
	_ = p.Connect("a", "c")

	res, err := p.Run(context.Background(), map[string][]int{"a": {1}})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := res["b"]; len(got) != 1 || got[0] != 11 {
		t.Fatalf("unexpected stage b outputs: %v", got)
	}
	if got := res["c"]; len(got) != 1 || got[0] != 101 {
		t.Fatalf("unexpected stage c outputs: %v", got)
	}
}

func TestRunMultipleSeeds(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})

	res, err := p.Run(context.Background(), map[string][]int{
		"a": {1, 2},
		"b": {3},
	})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := res["a"]; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("unexpected stage a outputs: %v", got)
	}
	if got := res["b"]; len(got) != 1 || got[0] != 3 {
		t.Fatalf("unexpected stage b outputs: %v", got)
	}
}

func TestRunStageErrorStops(t *testing.T) {
	p := NewPipeline[int]()
	wantErr := errors.New("boom")
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return nil, wantErr
		},
	})

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestRunContextCanceled(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Run(ctx, map[string][]int{"a": {1}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestRunContextCanceledMidRun(t *testing.T) {
	p := NewPipeline[int]()
	started := make(chan struct{}, 1)

	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := p.Run(ctx, map[string][]int{"a": {1}})
		done <- err
	}()

	select {
	case <-started:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("stage did not start in time")
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestRunFailFastReturnsStageError(t *testing.T) {
	p := NewPipeline[int]()
	wantErr := errors.New("boom")
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return nil, wantErr
		},
	})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2, 3, 4, 5, 6}},
		WithFailFast[int](true),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected stage error %v, got %v", wantErr, err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("expected stage error precedence over context cancellation, got %v", err)
	}
}

func TestRunNoFailFastContinuesAndReturnsJoinedError(t *testing.T) {
	p := NewPipeline[int]()
	var seen int64
	wantErr := errors.New("odd input")
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			atomic.AddInt64(&seen, 1)
			if in%2 == 1 {
				return nil, wantErr
			}
			return []int{in}, nil
		},
	})

	seeds := make([]int, 100)
	for i := range 100 {
		seeds[i] = i
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": seeds},
		WithFailFast[int](false),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected joined error to include %v, got %v", wantErr, err)
	}
	if got := atomic.LoadInt64(&seen); got != int64(len(seeds)) {
		t.Fatalf("expected all inputs to be processed without fail-fast, got %d want %d", got, len(seeds))
	}
}

func TestRunWithSmallBufferUnderLoad(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 4})
	_ = p.AddStage(testStage[int]{name: "b", workers: 4})
	_ = p.Connect("a", "b")

	const n = 1000
	seeds := make([]int, n)
	for i := range n {
		seeds[i] = i
	}

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": seeds},
		WithBufferSize[int](1),
	)
	if err != nil {
		t.Fatalf("unexpected run error with small buffer: %v", err)
	}
	if got := len(res["a"]); got != n {
		t.Fatalf("unexpected stage a result count: got %d want %d", got, n)
	}
	if got := len(res["b"]); got != n {
		t.Fatalf("unexpected stage b result count: got %d want %d", got, n)
	}
}

func TestRunStageErrorWinsOverExternalCancellation(t *testing.T) {
	p := NewPipeline[int]()
	wantErr := errors.New("stage failed")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil, wantErr
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := p.Run(ctx, map[string][]int{"a": {1}}, WithFailFast[int](true))
		done <- err
	}()

	select {
	case <-started:
		cancel()
		close(release)
	case <-time.After(2 * time.Second):
		t.Fatal("stage did not start in time")
	}

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected stage error %v, got %v", wantErr, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after error/cancellation interplay")
	}
}

func TestRunTriggerEmitsAndCompletes(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 10}, nil
		},
	})

	tr := testTrigger[int]{
		name:  "finite",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			for _, v := range []int{1, 2, 3} {
				if err := emit(v); err != nil {
					return err
				}
			}
			return nil
		},
	}

	res, err := p.Run(
		context.Background(),
		nil,
		WithTriggers[int](tr),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != 3 {
		t.Fatalf("unexpected stage a result count: got %d want %d", got, 3)
	}
}

func TestRunNonTerminatingTriggerExitsOnContextCancel(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	tr := testTrigger[int]{
		name:  "infinite",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := p.Run(ctx, nil, WithTriggers[int](tr))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRunMissingSeedStageUsesSentinel(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(context.Background(), map[string][]int{"missing": {1}})
	if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound, got %v", err)
	}
}

func TestRunMissingTriggerStageUsesSentinel(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	tr := testTrigger[int]{
		name:  "bad-trigger",
		stage: "missing",
		fn: func(ctx context.Context, emit func(int) error) error {
			return nil
		},
	}

	_, err := p.Run(context.Background(), nil, WithTriggers[int](tr))
	if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound, got %v", err)
	}
}

func TestRunTriggerErrorFailFastCancels(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	wantErr := errors.New("trigger failed")
	fail := testTrigger[int]{
		name:  "fail",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			return wantErr
		},
	}
	block := testTrigger[int]{
		name:  "block",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := p.Run(ctx, nil, WithFailFast[int](true), WithTriggers[int](fail, block))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected fail-fast to return trigger error %v, got %v", wantErr, err)
	}
}

func TestRunTriggerErrorNoFailFastContinues(t *testing.T) {
	p := NewPipeline[int]()
	var seen int64
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			atomic.AddInt64(&seen, 1)
			return []int{in}, nil
		},
	})

	wantErr := errors.New("trigger failed")
	fail := testTrigger[int]{
		name:  "fail",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			if err := emit(1); err != nil {
				return err
			}
			return wantErr
		},
	}
	ok := testTrigger[int]{
		name:  "ok",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			return emit(2)
		},
	}

	_, err := p.Run(context.Background(), nil, WithFailFast[int](false), WithTriggers[int](fail, ok))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected joined error to include %v, got %v", wantErr, err)
	}
	if got := atomic.LoadInt64(&seen); got != 2 {
		t.Fatalf("expected non-fail-fast to process both trigger emissions, got %d", got)
	}
}

func TestRunTriggerErrorReturnsPartialResultsWhenEnabled(t *testing.T) {
	p := NewPipeline[int]()
	var seen int64
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			atomic.AddInt64(&seen, 1)
			return []int{in}, nil
		},
	})

	wantErr := errors.New("trigger failed")
	fail := testTrigger[int]{
		name:  "fail",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			if err := emit(1); err != nil {
				return err
			}
			return wantErr
		},
	}
	ok := testTrigger[int]{
		name:  "ok",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			return emit(2)
		},
	}

	res, err := p.Run(
		context.Background(),
		nil,
		WithFailFast[int](false),
		WithPartialResults[int](true),
		WithTriggers[int](fail, ok),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected joined error to include %v, got %v", wantErr, err)
	}
	if res == nil {
		t.Fatal("expected partial results map, got nil")
	}
	if got := len(res["a"]); got != 2 {
		t.Fatalf("expected two partial results, got %d", got)
	}
	if got := atomic.LoadInt64(&seen); got != 2 {
		t.Fatalf("expected both trigger emissions to be processed, got %d", got)
	}
}

func TestRunSeedsAndTriggersMixedFlow(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 2})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	})
	_ = p.Connect("a", "b")

	tr := testTrigger[int]{
		name:  "t",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			if err := emit(3); err != nil {
				return err
			}
			return emit(4)
		},
	}

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2}},
		WithTriggers[int](tr),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != 4 {
		t.Fatalf("unexpected stage a result count: got %d want %d", got, 4)
	}
	if got := len(res["b"]); got != 4 {
		t.Fatalf("unexpected stage b result count: got %d want %d", got, 4)
	}
	sum := 0
	for _, v := range res["b"] {
		sum += v
	}
	if sum != 20 {
		t.Fatalf("unexpected stage b sum: got %d want %d", sum, 20)
	}
}

func TestRunSinkConsumesPerItem(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	})

	var (
		mu  sync.Mutex
		got []int
	)
	sink := testSink[int]{
		name:  "capture",
		stage: "a",
		fn: func(ctx context.Context, item int) error {
			mu.Lock()
			got = append(got, item)
			mu.Unlock()
			return nil
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2, 3}},
		WithSinks[int](sink),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("expected 3 sink items, got %d (%v)", len(got), got)
	}
}

func TestRunSinkRetriesUntilSuccess(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in}, nil
		},
	})

	var attempts atomic.Int64
	sink := testSink[int]{
		name:  "retry",
		stage: "a",
		fn: func(ctx context.Context, item int) error {
			if attempts.Add(1) <= 2 {
				return errors.New("transient sink error")
			}
			return nil
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {42}},
		WithSinks[int](sink),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := attempts.Load(); got < 3 {
		t.Fatalf("expected sink retries, attempts=%d", got)
	}
}

func TestRunPartialResultsOnStageError(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			if in < 0 {
				return nil, errors.New("bad input")
			}
			return []int{in}, nil
		},
	})

	// Default behavior: nil results on error.
	res, err := p.Run(context.Background(), map[string][]int{"a": {1, -1, 2}})
	if err == nil {
		t.Fatal("expected run error")
	}
	if res != nil {
		t.Fatalf("expected nil results without partial-results option, got %v", res)
	}

	// Partial-results behavior: results returned alongside error.
	res, err = p.Run(
		context.Background(),
		map[string][]int{"a": {1, -1, 2}},
		WithPartialResults[int](true),
	)
	if err == nil {
		t.Fatal("expected run error")
	}
	if res == nil {
		t.Fatal("expected partial results map, got nil")
	}
	if got := len(res["a"]); got == 0 {
		t.Fatalf("expected at least one successful partial output, got %d", got)
	}
}

func TestRunPartialResultsOnContextCancel(t *testing.T) {
	p := NewPipeline[int]()
	started := make(chan struct{}, 1)
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			// Allow one item to complete quickly, then block so cancellation happens mid-run.
			if in == 1 {
				select {
				case started <- struct{}{}:
				default:
				}
				return []int{in}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		res map[string][]int
		err error
	}, 1)

	go func() {
		res, err := p.Run(ctx, map[string][]int{"a": {1, 2}}, WithPartialResults[int](true))
		done <- struct {
			res map[string][]int
			err error
		}{res: res, err: err}
	}()

	select {
	case <-started:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("stage did not start in time")
	}

	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) {
			t.Fatalf("expected context canceled error, got %v", out.err)
		}
		if out.res == nil {
			t.Fatal("expected partial results map, got nil")
		}
		if got := len(out.res["a"]); got < 1 {
			t.Fatalf("expected at least one completed partial result, got %d", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
