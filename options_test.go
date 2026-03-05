package pipex

import (
	"context"
	"maps"
	"testing"
	"time"
)

func TestDefaultSinkRetryPolicyIsBounded(t *testing.T) {
	opts := defaultOptions[int]()
	if opts.SinkRetry.MaxRetries != 10 {
		t.Fatalf("unexpected default max retries: got %d want %d", opts.SinkRetry.MaxRetries, 10)
	}
	if opts.SinkRetry.Backoff <= 0 {
		t.Fatalf("expected positive default sink retry backoff, got %v", opts.SinkRetry.Backoff)
	}
}

func TestWithSinkRetryNormalizesZeroBackoff(t *testing.T) {
	opts := defaultOptions[int]()
	WithSinkRetry[int](3, 0)(opts)
	if opts.SinkRetry.MaxRetries != 3 {
		t.Fatalf("unexpected max retries: got %d want %d", opts.SinkRetry.MaxRetries, 3)
	}
	if opts.SinkRetry.Backoff != time.Millisecond {
		t.Fatalf("expected zero backoff normalized to 1ms, got %v", opts.SinkRetry.Backoff)
	}
}

func TestDefaultHooksAreZeroValue(t *testing.T) {
	opts := defaultOptions[int]()
	if opts.Hooks.RunStart != nil {
		t.Fatal("expected zero-value RunStart hook")
	}
	if opts.Hooks.RunEnd != nil {
		t.Fatal("expected zero-value RunEnd hook")
	}
	if opts.Hooks.StageStart != nil {
		t.Fatal("expected zero-value StageStart hook")
	}
}

func TestWithHooksSetsHooks(t *testing.T) {
	opts := defaultOptions[int]()
	called := false
	h := Hooks[int]{
		RunStart: func(ctx context.Context, meta RunMeta) {
			called = true
		},
	}

	WithHooks[int](h)(opts)

	if opts.Hooks.RunStart == nil {
		t.Fatal("expected RunStart hook to be set")
	}
	opts.Hooks.RunStart(context.Background(), RunMeta{})
	if !called {
		t.Fatal("expected stored hook callback to be callable")
	}
}

func TestWithCycleModeSetsOptions(t *testing.T) {
	opts := defaultOptions[int]()

	WithCycleMode[int](5, 200, func(v int) string { return "k" })(opts)

	if !opts.CycleMode.Enabled {
		t.Fatal("expected cycle mode enabled")
	}
	if opts.CycleMode.MaxHops != 5 {
		t.Fatalf("unexpected MaxHops: got %d want %d", opts.CycleMode.MaxHops, 5)
	}
	if opts.CycleMode.MaxJobs != 200 {
		t.Fatalf("unexpected MaxJobs: got %d want %d", opts.CycleMode.MaxJobs, 200)
	}
	if opts.CycleMode.DedupKey == nil {
		t.Fatal("expected DedupKey to be set")
	}
}

func TestWithStageWorkersSetsCopiedMap(t *testing.T) {
	opts := defaultOptions[int]()
	in := map[string]int{"a": 2}

	WithStageWorkers[int](in)(opts)

	if got := opts.StageWorkers["a"]; got != 2 {
		t.Fatalf("unexpected stage worker override: got %d want %d", got, 2)
	}

	in["a"] = 9
	if got := opts.StageWorkers["a"]; got != 2 {
		t.Fatalf("expected option map copy to be immutable from caller changes, got %d", got)
	}
	if maps.Equal(opts.StageWorkers, in) {
		t.Fatalf("expected copied map to diverge after caller mutation, got opts=%v in=%v", opts.StageWorkers, in)
	}
}

func TestWithStageWorkersLastCallWins(t *testing.T) {
	opts := defaultOptions[int]()

	WithStageWorkers[int](map[string]int{"a": 2, "b": 3})(opts)
	WithStageWorkers[int](map[string]int{"c": 4})(opts)

	if len(opts.StageWorkers) != 1 {
		t.Fatalf("expected replacement semantics, got %v", opts.StageWorkers)
	}
	if got := opts.StageWorkers["c"]; got != 4 {
		t.Fatalf("expected final override map to win, got c=%d map=%v", got, opts.StageWorkers)
	}
	if _, ok := opts.StageWorkers["a"]; ok {
		t.Fatalf("expected earlier map entries to be replaced, got %v", opts.StageWorkers)
	}
}
