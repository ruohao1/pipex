package pipex

import (
	"context"
	"errors"
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
	}

	if err := p.Connect("missing", "a"); err == nil {
		t.Fatalf("expected missing source stage error")
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
		WithFailFast(true),
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
		WithFailFast(false),
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
		WithBufferSize(1),
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
		_, err := p.Run(ctx, map[string][]int{"a": {1}}, WithFailFast(true))
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
