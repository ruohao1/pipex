package pipex

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

type mutableWorkersStage[T any] struct {
	name string
	w    atomic.Int64
	fn   func(context.Context, T) ([]T, error)
}

func newMutableWorkersStage[T any](name string, workers int, fn func(context.Context, T) ([]T, error)) *mutableWorkersStage[T] {
	s := &mutableWorkersStage[T]{name: name, fn: fn}
	s.w.Store(int64(workers))
	return s
}

func (s *mutableWorkersStage[T]) Name() string { return s.name }

func (s *mutableWorkersStage[T]) Workers() int { return int(s.w.Load()) }

func (s *mutableWorkersStage[T]) SetWorkers(n int) { s.w.Store(int64(n)) }

func (s *mutableWorkersStage[T]) Process(ctx context.Context, in T) (out []T, err error) {
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

func TestConnectSelfLoopAllowedAndValidateDetectsCycle(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	if err := p.Connect("a", "a"); err != nil {
		t.Fatalf("expected self-loop connect to succeed, got %v", err)
	}
	if err := p.Validate(); err != ErrCycle {
		t.Fatalf("expected Validate to return ErrCycle for self-loop, got %v", err)
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

func TestRunMutableWorkersCanBreakPoolCreationAfterAddStage(t *testing.T) {
	p := NewPipeline[int]()
	stage := newMutableWorkersStage[int]("a", 1, nil)
	if err := p.AddStage(stage); err != nil {
		t.Fatalf("unexpected add stage error: %v", err)
	}

	stage.SetWorkers(0)

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}})
	if !errors.Is(err, ErrInvalidWorkerCount) {
		t.Fatalf("expected ErrInvalidWorkerCount after mutating Workers(), got %v", err)
	}
	if got := err.Error(); !strings.Contains(got, "failed to create worker pool") {
		t.Fatalf("expected pool-creation context in error, got %v", err)
	}
}

func TestRunStageWorkersOverrideApplied(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2, 3}},
		WithStageWorkers[int](map[string]int{"a": 3}),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != 3 {
		t.Fatalf("unexpected outputs with stage-worker override: got %d want %d", got, 3)
	}
}

func TestRunStageWorkersOverrideUnknownStageRejected(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageWorkers[int](map[string]int{"missing": 2}),
	)
	if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound for unknown stage override, got %v", err)
	}
}

func TestRunStageWorkersOverrideInvalidCountRejected(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageWorkers[int](map[string]int{"a": 0}),
	)
	if !errors.Is(err, ErrInvalidWorkerCount) {
		t.Fatalf("expected ErrInvalidWorkerCount for invalid stage-worker override, got %v", err)
	}
}

func TestRunStageWorkersLastOptionReplacesEarlierMap(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2}},
		WithStageWorkers[int](map[string]int{"missing": 2}),
		WithStageWorkers[int](map[string]int{"a": 2}),
	)
	if err != nil {
		t.Fatalf("expected final stage-workers override map to replace earlier one, got %v", err)
	}
	if got := len(res["a"]); got != 2 {
		t.Fatalf("unexpected outputs with replacement semantics: got %d want %d", got, 2)
	}
}

func TestRunStageRateLimitsUnknownStageRejected(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageRateLimits[int](map[string]RateLimit{"missing": {RPS: 10, Burst: 1}}),
	)
	if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound for unknown stage rate-limit override, got %v", err)
	}
}

func TestRunStagePoliciesUnknownStageRejected(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStagePolicies[int](map[string]StagePolicy{
			"missing": {
				MaxAttempts: 2,
				Backoff:     5 * time.Millisecond,
				Timeout:     10 * time.Millisecond,
			},
		}),
	)
	if !errors.Is(err, ErrStageNotFound) {
		t.Fatalf("expected ErrStageNotFound for unknown stage policy override, got %v", err)
	}
}

func TestRunStageRateLimitsInvalidRPSRejected(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageRateLimits[int](map[string]RateLimit{"a": {RPS: 0, Burst: 1}}),
	)
	if !errors.Is(err, ErrInvalidRPS) {
		t.Fatalf("expected ErrInvalidRPS for invalid stage rate-limit override, got %v", err)
	}
}

func TestRunStageRateLimitsInvalidBurstRejected(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithStageRateLimits[int](map[string]RateLimit{"a": {RPS: 10, Burst: 0}}),
	)
	if !errors.Is(err, ErrInvalidBurst) {
		t.Fatalf("expected ErrInvalidBurst for invalid stage rate-limit override, got %v", err)
	}
}

func TestRunStageRateLimitsLastOptionReplacesEarlierMap(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2}},
		WithStageRateLimits[int](map[string]RateLimit{"missing": {RPS: 10, Burst: 1}}),
		WithStageRateLimits[int](map[string]RateLimit{"a": {RPS: 1000, Burst: 1}}),
	)
	if err != nil {
		t.Fatalf("expected final stage-rate-limits map to replace earlier one, got %v", err)
	}
	if got := len(res["a"]); got != 2 {
		t.Fatalf("unexpected outputs with replacement semantics: got %d want %d", got, 2)
	}
}

func TestRunStageRateLimitWaitHonorsContextCancellation(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	_, err := p.Run(
		ctx,
		map[string][]int{"a": {1, 2}},
		WithStageRateLimits[int](map[string]RateLimit{"a": {RPS: 0.5, Burst: 1}}),
	)
	if err == nil {
		t.Fatal("expected limiter wait failure under tight context deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, context.Canceled) &&
		!strings.Contains(err.Error(), "would exceed context deadline") {
		t.Fatalf("expected context/limiter deadline error while waiting on stage limiter, got %v", err)
	}
}

func TestRunStageRateLimitsRepeatedCancellationStability(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in}, nil
		},
	})

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		_, err := p.Run(
			ctx,
			map[string][]int{"a": {1, 2}},
			WithStageRateLimits[int](map[string]RateLimit{"a": {RPS: 0.5, Burst: 1}}),
		)
		cancel()

		if err == nil {
			t.Fatalf("iteration %d: expected limiter wait failure under tight context deadline", i)
		}
		if !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) &&
			!strings.Contains(err.Error(), "would exceed context deadline") {
			t.Fatalf("iteration %d: expected context/limiter deadline error, got %v", i, err)
		}
	}
}

func TestRunDedupRuleStageScopeAppliesOnlyToTargetStage(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.AddStage(testStage[int]{name: "c", workers: 1})

	res, err := p.Run(
		context.Background(),
		map[string][]int{
			"b": {1, 1, 2},
			"c": {1, 1, 2},
		},
		WithDedupRules[int](DedupRule[int]{
			Name:  "b-only",
			Scope: DedupScopeStage("b"),
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["b"]); got != 2 {
		t.Fatalf("expected dedup on stage b only, got b=%v", res["b"])
	}
	if got := len(res["c"]); got != 3 {
		t.Fatalf("expected no dedup on stage c for stage-scoped rule, got c=%v", res["c"])
	}
}

func TestRunDedupRuleGlobalAppliesToAllStages(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.AddStage(testStage[int]{name: "c", workers: 1})

	res, err := p.Run(
		context.Background(),
		map[string][]int{
			"b": {1, 1, 2},
			"c": {1, 1, 2},
		},
		WithDedupRules[int](DedupRule[int]{
			Name:  "all-stages",
			Scope: DedupScopeGlobal,
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["b"]); got != 2 {
		t.Fatalf("expected dedup on stage b for global rule, got b=%v", res["b"])
	}
	if got := len(res["c"]); got != 2 {
		t.Fatalf("expected dedup on stage c for global rule, got c=%v", res["c"])
	}
}

func TestRunDedupRollbackOnEnqueueFailureDoesNotLeakKey(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			// Keep job running so downstream enqueue attempts happen after context timeout.
			time.Sleep(25 * time.Millisecond)
			return []int{in}, nil
		},
	})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")

	// Run 1: timeout after dedup insert but before enqueue send succeeds.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel1()
	_, err := p.Run(
		ctx1,
		map[string][]int{"a": {1}},
		WithDedupRules[int](DedupRule[int]{
			Name:  "b-host",
			Scope: DedupScopeStage("b"),
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected timeout/cancel in run1, got %v", err)
	}

	// Run 2: same item should not be falsely dropped by leaked dedup key from run1.
	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithDedupRules[int](DedupRule[int]{
			Name:  "b-host",
			Scope: DedupScopeStage("b"),
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if err != nil {
		t.Fatalf("unexpected run2 error: %v", err)
	}
	if got := len(res["b"]); got != 1 {
		t.Fatalf("expected downstream enqueue to succeed in run2 (no leaked dedup key), got b=%v", res["b"])
	}
}

func TestRunDedupRollbackOnMaxHopsDropDoesNotLeakKey(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "s",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			// Delay short path so long path reaches target stage first.
			time.Sleep(25 * time.Millisecond)
			return []int{in}, nil
		},
	})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.AddStage(testStage[int]{name: "c", workers: 1})
	_ = p.AddStage(testStage[int]{name: "t", workers: 1})

	_ = p.Connect("s", "a") // s->a->t is hops=2 (allowed)
	_ = p.Connect("s", "b") // s->b->c->t is hops=3 (dropped by max hops)
	_ = p.Connect("a", "t")
	_ = p.Connect("b", "c")
	_ = p.Connect("c", "t")

	res, err := p.Run(
		context.Background(),
		map[string][]int{"s": {1}},
		WithCycleMode[int](2, 100),
		WithDedupRules[int](DedupRule[int]{
			Name:  "t-only",
			Scope: DedupScopeStage("t"),
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["t"]); got != 1 {
		t.Fatalf("expected target stage to process once after hop-limit drop rollback, got t=%v", res["t"])
	}
}

func TestRunDedupRollbackOnLaterRulePanicDoesNotLeakEarlierRuleKey(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	var panicOnce atomic.Bool
	panicOnce.Store(true)

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 1}},
		WithPartialResults[int](true),
		WithDedupRules[int](
			DedupRule[int]{
				Name:  "first",
				Scope: DedupScopeStage("a"),
				Key: func(v int) string {
					return fmt.Sprintf("%d", v)
				},
			},
			DedupRule[int]{
				Name:  "panic-once",
				Scope: DedupScopeStage("a"),
				Key: func(v int) string {
					if panicOnce.CompareAndSwap(true, false) {
						panic("boom")
					}
					return fmt.Sprintf("%d", v)
				},
			},
		),
	)
	if err == nil {
		t.Fatal("expected dedup panic error")
	}
	if got := err.Error(); !strings.Contains(got, "dedup key panic") {
		t.Fatalf("expected dedup panic error, got %v", err)
	}
	if res == nil {
		t.Fatal("expected partial results map, got nil")
	}
	if got := len(res["a"]); got != 1 {
		t.Fatalf("expected second item to process after rollback from first-item panic, got a=%v", res["a"])
	}
}

func TestRunFailFastWithQueuedJobsDoesNotHang(t *testing.T) {
	p := NewPipeline[int]()
	wantErr := errors.New("boom")
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			if in == 0 {
				return nil, wantErr
			}
			return []int{in}, nil
		},
	})

	seeds := make([]int, 400)
	for i := range seeds {
		seeds[i] = i
	}

	done := make(chan error, 1)
	go func() {
		_, err := p.Run(
			context.Background(),
			map[string][]int{"a": seeds},
			WithFailFast[int](true),
			WithBufferSize[int](512),
		)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected fail-fast stage error %v, got %v", wantErr, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run appears deadlocked after fail-fast cancellation with queued jobs")
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

func TestRunSinkRetryExhaustedReturnsError(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})

	sink := testSink[int]{
		name:  "always-fail",
		stage: "a",
		fn: func(ctx context.Context, item int) error {
			return errors.New("sink down")
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1, 2, 3}},
		WithSinks[int](sink),
		WithSinkRetry[int](1, time.Millisecond),
	)
	if err == nil {
		t.Fatal("expected sink retry exhaustion error")
	}
	if got := err.Error(); !strings.Contains(got, "retries exhausted") || !strings.Contains(got, "sink down") {
		t.Fatalf("expected retries exhausted sink error, got %v", err)
	}
}

func TestRunSinkRetryExhaustedFailFastCancels(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			time.Sleep(2 * time.Millisecond)
			return []int{in}, nil
		},
	})

	sink := testSink[int]{
		name:  "always-fail",
		stage: "a",
		fn: func(ctx context.Context, item int) error {
			return errors.New("sink fail")
		},
	}

	seeds := make([]int, 100)
	for i := range seeds {
		seeds[i] = i
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": seeds},
		WithSinks[int](sink),
		WithSinkRetry[int](0, time.Millisecond),
		WithFailFast[int](true),
	)
	if err == nil {
		t.Fatal("expected sink failure under fail-fast")
	}
	if got := err.Error(); !strings.Contains(got, "retries exhausted") || !strings.Contains(got, "sink fail") {
		t.Fatalf("expected sink retry exhaustion error, got %v", err)
	}
}

func TestRunSlowSinkHighThroughputNoDeadlock(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in}, nil
		},
	})

	const n = 500
	seeds := make([]int, n)
	for i := range seeds {
		seeds[i] = i
	}

	var seen atomic.Int64
	sink := testSink[int]{
		name:  "slow",
		stage: "a",
		fn: func(ctx context.Context, item int) error {
			time.Sleep(1 * time.Millisecond)
			seen.Add(1)
			return nil
		},
	}

	done := make(chan struct {
		res map[string][]int
		err error
	}, 1)
	go func() {
		res, err := p.Run(
			context.Background(),
			map[string][]int{"a": seeds},
			WithSinks[int](sink),
			WithBufferSize[int](32),
		)
		done <- struct {
			res map[string][]int
			err error
		}{res: res, err: err}
	}()

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("unexpected run error: %v", out.err)
		}
		if got := len(out.res["a"]); got != n {
			t.Fatalf("unexpected stage output count: got %d want %d", got, n)
		}
		if got := seen.Load(); got != int64(n) {
			t.Fatalf("unexpected sink item count: got %d want %d", got, n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run appears deadlocked under slow sink load")
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

func TestRunCycleGraphRejectedByDefault(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	_, err := p.Run(context.Background(), map[string][]int{"a": {1}})
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle by default, got %v", err)
	}
}

func TestRunCycleModeAllowsCycleWithMaxHops(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithCycleMode[int](2, 100),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != 2 {
		t.Fatalf("unexpected stage a count: got %d want %d", got, 2)
	}
	if got := len(res["b"]); got != 1 {
		t.Fatalf("unexpected stage b count: got %d want %d", got, 1)
	}
}

func TestRunCycleModeMaxJobsExceeded(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithCycleMode[int](-1, 2),
		WithFailFast[int](true),
	)
	if !errors.Is(err, ErrCycleModeMaxJobsExceeded) {
		t.Fatalf("expected ErrCycleModeMaxJobsExceeded, got %v", err)
	}
}

func TestRunCycleModeDedupStopsRevisit(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithCycleMode[int](-1, 100),
		WithDedupRules[int](DedupRule[int]{
			Name:  "cycle-item",
			Scope: DedupScopeGlobal,
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != 1 {
		t.Fatalf("unexpected stage a count with dedup: got %d want %d", got, 1)
	}
	if got := len(res["b"]); got != 1 {
		t.Fatalf("unexpected stage b count with dedup: got %d want %d", got, 1)
	}
}

func TestRunCycleModeAndDedupRulesNoCrossRuleCollision(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	res, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithCycleMode[int](-1, 100),
		WithDedupRules[int](DedupRule[int]{
			Name:  "global-dedup",
			Scope: DedupScopeGlobal,
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := len(res["a"]); got != 1 {
		t.Fatalf("expected stage a to process once, got %v", res["a"])
	}
	if got := len(res["b"]); got != 1 {
		t.Fatalf("expected stage b to process once, got %v", res["b"])
	}
}

func TestRunCycleModeDedupRuleKeyPanicReturnsError(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	done := make(chan error, 1)
	go func() {
		_, err := p.Run(
			context.Background(),
			map[string][]int{"a": {1}},
			WithCycleMode[int](-1, 100),
			WithDedupRules[int](DedupRule[int]{
				Name:  "panic-key",
				Scope: DedupScopeGlobal,
				Key: func(v int) string {
					panic("dedup panic")
				},
			}),
		)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected dedup key panic to surface as run error")
		}
		if got := err.Error(); !strings.Contains(got, "dedup key panic") {
			t.Fatalf("expected dedup panic error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run appears stuck after dedup key panic")
	}
}

func TestRunCycleModeMaxJobsBoundUnderConcurrentEnqueue(t *testing.T) {
	const (
		maxJobs    = 20
		iterations = 20
	)

	for i := 0; i < iterations; i++ {
		p := NewPipeline[int]()
		_ = p.AddStage(testStage[int]{
			name:    "a",
			workers: 8,
			fn: func(ctx context.Context, in int) ([]int, error) {
				return []int{in}, nil
			},
		})
		_ = p.AddStage(testStage[int]{
			name:    "b",
			workers: 8,
			fn: func(ctx context.Context, in int) ([]int, error) {
				return []int{in}, nil
			},
		})
		_ = p.Connect("a", "b")
		_ = p.Connect("b", "a")

		res, err := p.Run(
			context.Background(),
			map[string][]int{"a": {1}},
			WithCycleMode[int](-1, maxJobs),
			WithFailFast[int](true),
			WithPartialResults[int](true),
		)
		if !errors.Is(err, ErrCycleModeMaxJobsExceeded) {
			t.Fatalf("iteration %d: expected ErrCycleModeMaxJobsExceeded, got %v", i, err)
		}

		totalProcessed := len(res["a"]) + len(res["b"])
		if totalProcessed > maxJobs {
			t.Fatalf("iteration %d: processed jobs exceeded maxJobs: got %d want <= %d", i, totalProcessed, maxJobs)
		}
	}
}

func TestRunFrontierParityDedup(t *testing.T) {
	for _, useFrontier := range []bool{false, true} {
		t.Run(fmt.Sprintf("frontier=%v", useFrontier), func(t *testing.T) {
			p := NewPipeline[int]()
			_ = p.AddStage(testStage[int]{name: "a", workers: 2})

			res, err := p.Run(
				context.Background(),
				map[string][]int{"a": {1, 1, 2, 2, 3}},
				WithFrontier[int](useFrontier),
				WithDedupRules[int](DedupRule[int]{
					Name:  "uniq",
					Scope: DedupScopeGlobal,
					Key: func(v int) string {
						return fmt.Sprintf("%d", v)
					},
				}),
			)
			if err != nil {
				t.Fatalf("unexpected run error: %v", err)
			}
			if got := len(res["a"]); got != 3 {
				t.Fatalf("unexpected dedup count: got %d want 3", got)
			}
		})
	}
}

func TestRunFrontierParityCycleMaxHops(t *testing.T) {
	for _, useFrontier := range []bool{false, true} {
		t.Run(fmt.Sprintf("frontier=%v", useFrontier), func(t *testing.T) {
			p := NewPipeline[int]()
			_ = p.AddStage(testStage[int]{name: "a", workers: 1})
			_ = p.AddStage(testStage[int]{name: "b", workers: 1})
			_ = p.Connect("a", "b")
			_ = p.Connect("b", "a")

			res, err := p.Run(
				context.Background(),
				map[string][]int{"a": {1}},
				WithFrontier[int](useFrontier),
				WithCycleMode[int](2, 100),
			)
			if err != nil {
				t.Fatalf("unexpected run error: %v", err)
			}
			if got := len(res["a"]); got != 2 {
				t.Fatalf("unexpected stage a count: got %d want 2", got)
			}
			if got := len(res["b"]); got != 1 {
				t.Fatalf("unexpected stage b count: got %d want 1", got)
			}
		})
	}
}

func TestRunFrontierParityCycleMaxJobsExceeded(t *testing.T) {
	for _, useFrontier := range []bool{false, true} {
		t.Run(fmt.Sprintf("frontier=%v", useFrontier), func(t *testing.T) {
			p := NewPipeline[int]()
			_ = p.AddStage(testStage[int]{name: "a", workers: 1})
			_ = p.AddStage(testStage[int]{name: "b", workers: 1})
			_ = p.Connect("a", "b")
			_ = p.Connect("b", "a")

			_, err := p.Run(
				context.Background(),
				map[string][]int{"a": {1}},
				WithFrontier[int](useFrontier),
				WithCycleMode[int](-1, 2),
				WithFailFast[int](true),
			)
			if !errors.Is(err, ErrCycleModeMaxJobsExceeded) {
				t.Fatalf("expected ErrCycleModeMaxJobsExceeded, got %v", err)
			}
		})
	}
}

func TestRunFrontierParityCancellation(t *testing.T) {
	for _, useFrontier := range []bool{false, true} {
		t.Run(fmt.Sprintf("frontier=%v", useFrontier), func(t *testing.T) {
			p := NewPipeline[int]()
			_ = p.AddStage(testStage[int]{
				name:    "a",
				workers: 1,
				fn: func(ctx context.Context, in int) ([]int, error) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(200 * time.Millisecond):
						return []int{in}, nil
					}
				},
			})

			runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()

			_, err := p.Run(
				runCtx,
				map[string][]int{"a": {1}},
				WithFrontier[int](useFrontier),
				WithFailFast[int](true),
			)
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				t.Fatalf("expected cancellation error, got %v", err)
			}
		})
	}
}
