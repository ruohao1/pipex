package pipex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCycleHookHopLimitDropPayload(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")

	var (
		mu       sync.Mutex
		runID    string
		dropSeen bool
		dropEvt  CycleHopLimitDropEvent[int]
	)

	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			mu.Lock()
			runID = meta.RunID
			mu.Unlock()
		},
		CycleHopLimitDrop: func(ctx context.Context, e CycleHopLimitDropEvent[int]) {
			mu.Lock()
			dropSeen = true
			dropEvt = e
			mu.Unlock()
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithCycleMode[int](0, 100),
		WithHooks[int](hooks),
	)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !dropSeen {
		t.Fatal("expected CycleHopLimitDrop hook event")
	}
	if dropEvt.RunID == "" || dropEvt.RunID != runID {
		t.Fatalf("unexpected RunID: event=%q run=%q", dropEvt.RunID, runID)
	}
	if dropEvt.Stage != "b" {
		t.Fatalf("unexpected drop stage: got %q want %q", dropEvt.Stage, "b")
	}
	if dropEvt.Item != 1 {
		t.Fatalf("unexpected drop item: got %d want %d", dropEvt.Item, 1)
	}
	if dropEvt.Hops != 1 || dropEvt.MaxHops != 0 {
		t.Fatalf("unexpected hop fields: hops=%d max=%d", dropEvt.Hops, dropEvt.MaxHops)
	}
}

func TestCycleModeWithDedupRulesEmitsSingleDedupHookPath(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	var dedupDrops atomic.Int64

	hooks := Hooks[int]{
		DedupDrop: func(ctx context.Context, e DedupDropEvent[int]) {
			dedupDrops.Add(1)
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithHooks[int](hooks),
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
	if dedupDrops.Load() == 0 {
		t.Fatal("expected dedup drops to be reported via DedupDrop when explicit dedup rule is configured")
	}
}

func TestCycleHookMaxJobsExceededPayload(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{name: "a", workers: 1})
	_ = p.AddStage(testStage[int]{name: "b", workers: 1})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	var (
		mu      sync.Mutex
		runID   string
		evtSeen bool
		evt     CycleMaxJobsExceededEvent[int]
	)

	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			mu.Lock()
			runID = meta.RunID
			mu.Unlock()
		},
		CycleMaxJobsExceeded: func(ctx context.Context, e CycleMaxJobsExceededEvent[int]) {
			mu.Lock()
			evtSeen = true
			evt = e
			mu.Unlock()
		},
	}

	_, err := p.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		WithCycleMode[int](-1, 1),
		WithFailFast[int](true),
		WithHooks[int](hooks),
	)
	if !errors.Is(err, ErrCycleModeMaxJobsExceeded) {
		t.Fatalf("expected ErrCycleModeMaxJobsExceeded, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !evtSeen {
		t.Fatal("expected CycleMaxJobsExceeded hook event")
	}
	if evt.RunID == "" || evt.RunID != runID {
		t.Fatalf("unexpected RunID: event=%q run=%q", evt.RunID, runID)
	}
	if evt.Stage != "b" {
		t.Fatalf("unexpected stage: got %q want %q", evt.Stage, "b")
	}
	if evt.Item != 1 {
		t.Fatalf("unexpected item: got %d want %d", evt.Item, 1)
	}
	if evt.AcceptedJobs != 1 || evt.MaxJobs != 1 {
		t.Fatalf("unexpected max-jobs fields: accepted=%d max=%d", evt.AcceptedJobs, evt.MaxJobs)
	}
}

func TestCycleModeHooksWithTriggersConcurrent(t *testing.T) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 6,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 6,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in}, nil
		},
	})
	_ = p.Connect("a", "b")
	_ = p.Connect("b", "a")

	tr := testTrigger[int]{
		name:  "burst",
		stage: "a",
		fn: func(ctx context.Context, emit func(int) error) error {
			for i := 0; i < 200; i++ {
				if err := emit(i % 3); err != nil {
					return err
				}
			}
			return nil
		},
	}

	var (
		runStart atomic.Int64
		runEnd   atomic.Int64
		dedup    atomic.Int64
		hopDrop  atomic.Int64
	)
	hooks := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			runStart.Add(1)
		},
		RunEnd: func(ctx context.Context, meta RunMeta, err error) {
			runEnd.Add(1)
		},
		DedupDrop: func(ctx context.Context, e DedupDropEvent[int]) {
			dedup.Add(1)
		},
		CycleHopLimitDrop: func(ctx context.Context, e CycleHopLimitDropEvent[int]) {
			hopDrop.Add(1)
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := p.Run(
		ctx,
		map[string][]int{"a": {1, 2, 3}},
		WithTriggers[int](tr),
		WithHooks[int](hooks),
		WithCycleMode[int](4, 1000),
		WithDedupRules[int](DedupRule[int]{
			Name:  "global",
			Scope: DedupScopeGlobal,
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		}),
		WithPartialResults[int](true),
	)
	if err != nil && !errors.Is(err, ErrCycleModeMaxJobsExceeded) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected run error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil results map")
	}

	if got := runStart.Load(); got != 1 {
		t.Fatalf("expected RunStart once, got %d", got)
	}
	if got := runEnd.Load(); got != 1 {
		t.Fatalf("expected RunEnd once, got %d", got)
	}
	if dedup.Load() == 0 && hopDrop.Load() == 0 {
		t.Fatal("expected at least one cycle guardrail hook event")
	}
}
