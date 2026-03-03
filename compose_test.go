package pipex

import (
	"context"
	"errors"
	"slices"
	"testing"
)

type altTestStage[T any] struct {
	name    string
	workers int
}

func (s altTestStage[T]) Name() string { return s.name }

func (s altTestStage[T]) Workers() int { return s.workers }

func (s altTestStage[T]) Process(ctx context.Context, in T) ([]T, error) {
	return []T{in}, nil
}

func TestSequenceBuildsAndRunsChain(t *testing.T) {
	p := NewPipeline[int]()
	a := testStage[int]{
		name:    "a",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	}
	b := testStage[int]{
		name:    "b",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	}
	c := testStage[int]{
		name:    "c",
		workers: 1,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in - 3}, nil
		},
	}

	if err := p.Sequence(a, b, c); err != nil {
		t.Fatalf("unexpected sequence error: %v", err)
	}

	if !slices.Contains(p.edges["a"], "b") {
		t.Fatalf("missing edge a->b: %v", p.edges["a"])
	}
	if !slices.Contains(p.edges["b"], "c") {
		t.Fatalf("missing edge b->c: %v", p.edges["b"])
	}

	res, err := p.Run(context.Background(), map[string][]int{"a": {1}})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if got := res["c"]; len(got) != 1 || got[0] != 1 {
		t.Fatalf("unexpected stage c outputs: %v", got)
	}
}

func TestSequenceIdempotentOnExistingStagesAndEdges(t *testing.T) {
	p := NewPipeline[int]()
	a := testStage[int]{name: "a", workers: 1}
	b := testStage[int]{name: "b", workers: 1}

	if err := p.Sequence(a, b); err != nil {
		t.Fatalf("unexpected first sequence error: %v", err)
	}
	if err := p.Sequence(a, b); err != nil {
		t.Fatalf("unexpected second sequence error: %v", err)
	}

	if got := len(p.edges["a"]); got != 1 {
		t.Fatalf("expected one edge from a, got %d (%v)", got, p.edges["a"])
	}
}

func TestSequenceErrors(t *testing.T) {
	p := NewPipeline[int]()
	a := testStage[int]{name: "a", workers: 1}

	if err := p.Sequence(a); !errors.Is(err, ErrNotEnoughStages) {
		t.Fatalf("expected ErrNotEnoughStages, got %v", err)
	}
	if err := p.Sequence(a, nil); !errors.Is(err, ErrNilStage) {
		t.Fatalf("expected ErrNilStage, got %v", err)
	}
}

func TestSequenceConflictSameNameDifferentDefinition(t *testing.T) {
	p := NewPipeline[int]()
	a := testStage[int]{name: "a", workers: 1}
	b := testStage[int]{name: "b", workers: 1}

	if err := p.Sequence(a, b); err != nil {
		t.Fatalf("unexpected setup error: %v", err)
	}

	conflict := altTestStage[int]{name: "a", workers: 1}
	if err := p.Sequence(conflict, b); !errors.Is(err, ErrStageConflict) {
		t.Fatalf("expected ErrStageConflict, got %v", err)
	}
}

func TestParallelAddsIndependentStages(t *testing.T) {
	p := NewPipeline[int]()
	a := testStage[int]{name: "a", workers: 1}
	b := testStage[int]{name: "b", workers: 1}

	if err := p.Parallel(a, b); err != nil {
		t.Fatalf("unexpected parallel error: %v", err)
	}
	if len(p.edges["a"]) != 0 || len(p.edges["b"]) != 0 {
		t.Fatalf("expected no edges for parallel stages, got a=%v b=%v", p.edges["a"], p.edges["b"])
	}
}

func TestParallelErrors(t *testing.T) {
	p := NewPipeline[int]()

	if err := p.Parallel(nil); !errors.Is(err, ErrNilStage) {
		t.Fatalf("expected ErrNilStage, got %v", err)
	}
}

func TestParallelConflictSameNameDifferentDefinition(t *testing.T) {
	p := NewPipeline[int]()
	a := testStage[int]{name: "a", workers: 1}
	if err := p.Parallel(a); err != nil {
		t.Fatalf("unexpected setup error: %v", err)
	}

	conflict := altTestStage[int]{name: "a", workers: 1}
	if err := p.Parallel(conflict); !errors.Is(err, ErrStageConflict) {
		t.Fatalf("expected ErrStageConflict, got %v", err)
	}
}

func TestMergeConnectsInputsToOutput(t *testing.T) {
	p := NewPipeline[int]()
	inA := testStage[int]{name: "inA", workers: 1}
	inB := testStage[int]{name: "inB", workers: 1}
	out := testStage[int]{name: "out", workers: 1}

	if err := p.Merge([]Stage[int]{inA, inB}, out); err != nil {
		t.Fatalf("unexpected merge error: %v", err)
	}

	if !slices.Contains(p.edges["inA"], "out") {
		t.Fatalf("missing edge inA->out: %v", p.edges["inA"])
	}
	if !slices.Contains(p.edges["inB"], "out") {
		t.Fatalf("missing edge inB->out: %v", p.edges["inB"])
	}
}

func TestMergeErrors(t *testing.T) {
	p := NewPipeline[int]()
	inA := testStage[int]{name: "inA", workers: 1}
	out := testStage[int]{name: "out", workers: 1}

	if err := p.Merge(nil, out); !errors.Is(err, ErrNotEnoughStages) {
		t.Fatalf("expected ErrNotEnoughStages, got %v", err)
	}
	if err := p.Merge([]Stage[int]{inA}, nil); !errors.Is(err, ErrNilStage) {
		t.Fatalf("expected ErrNilStage for output, got %v", err)
	}
	if err := p.Merge([]Stage[int]{nil}, out); !errors.Is(err, ErrNilStage) {
		t.Fatalf("expected ErrNilStage for input, got %v", err)
	}
}

func TestMergeConflictSameNameDifferentDefinition(t *testing.T) {
	p := NewPipeline[int]()
	in := testStage[int]{name: "in", workers: 1}
	out := testStage[int]{name: "out", workers: 1}
	if err := p.Merge([]Stage[int]{in}, out); err != nil {
		t.Fatalf("unexpected setup error: %v", err)
	}

	conflictOut := altTestStage[int]{name: "out", workers: 1}
	if err := p.Merge([]Stage[int]{in}, conflictOut); !errors.Is(err, ErrStageConflict) {
		t.Fatalf("expected ErrStageConflict, got %v", err)
	}
}

func TestSplitConnectsInputToAllOutputs(t *testing.T) {
	p := NewPipeline[int]()
	in := testStage[int]{name: "in", workers: 1}
	outA := testStage[int]{name: "outA", workers: 1}
	outB := testStage[int]{name: "outB", workers: 1}

	if err := p.Split(in, outA, outB); err != nil {
		t.Fatalf("unexpected split error: %v", err)
	}
	if !slices.Contains(p.edges["in"], "outA") || !slices.Contains(p.edges["in"], "outB") {
		t.Fatalf("missing split edges: %v", p.edges["in"])
	}
}

func TestSplitErrors(t *testing.T) {
	p := NewPipeline[int]()
	in := testStage[int]{name: "in", workers: 1}

	if err := p.Split(nil, in); !errors.Is(err, ErrNilStage) {
		t.Fatalf("expected ErrNilStage for input, got %v", err)
	}
	if err := p.Split(in); !errors.Is(err, ErrNotEnoughStages) {
		t.Fatalf("expected ErrNotEnoughStages, got %v", err)
	}
	if err := p.Split(in, nil); !errors.Is(err, ErrNilStage) {
		t.Fatalf("expected ErrNilStage for output, got %v", err)
	}
}

func TestSplitConflictSameNameDifferentDefinition(t *testing.T) {
	p := NewPipeline[int]()
	in := testStage[int]{name: "in", workers: 1}
	out := testStage[int]{name: "out", workers: 1}
	if err := p.Split(in, out); err != nil {
		t.Fatalf("unexpected setup error: %v", err)
	}

	conflictOut := altTestStage[int]{name: "out", workers: 1}
	if err := p.Split(in, conflictOut); !errors.Is(err, ErrStageConflict) {
		t.Fatalf("expected ErrStageConflict, got %v", err)
	}
}

func TestComposeSequenceStressManySeedsMultiWorkers(t *testing.T) {
	const n = 1000

	p := NewPipeline[int]()
	a := testStage[int]{
		name:    "a",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	}
	b := testStage[int]{
		name:    "b",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	}
	c := testStage[int]{
		name:    "c",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in - 3}, nil
		},
	}
	if err := p.Sequence(a, b, c); err != nil {
		t.Fatalf("unexpected sequence error: %v", err)
	}

	seeds := make([]int, n)
	for i := range n {
		seeds[i] = i
	}
	res, err := p.Run(context.Background(), map[string][]int{"a": seeds})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if got := len(res["a"]); got != n {
		t.Fatalf("unexpected stage a result count: %d", got)
	}
	if got := len(res["b"]); got != n {
		t.Fatalf("unexpected stage b result count: %d", got)
	}
	if got := len(res["c"]); got != n {
		t.Fatalf("unexpected stage c result count: %d", got)
	}

	sum := 0
	for _, v := range res["c"] {
		sum += v
	}
	want := n * (n - 2)
	if sum != want {
		t.Fatalf("unexpected stage c sum: got %d want %d", sum, want)
	}
}

func TestComposeSplitMergeStressManySeedsMultiWorkers(t *testing.T) {
	const n = 1000

	p := NewPipeline[int]()
	src := testStage[int]{name: "src", workers: 4}
	left := testStage[int]{
		name:    "left",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	}
	right := testStage[int]{
		name:    "right",
		workers: 4,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 2}, nil
		},
	}
	sink := testStage[int]{name: "sink", workers: 4}

	if err := p.Split(src, left, right); err != nil {
		t.Fatalf("unexpected split error: %v", err)
	}
	if err := p.Merge([]Stage[int]{left, right}, sink); err != nil {
		t.Fatalf("unexpected merge error: %v", err)
	}

	seeds := make([]int, n)
	for i := range n {
		seeds[i] = i
	}
	res, err := p.Run(context.Background(), map[string][]int{"src": seeds})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if got := len(res["sink"]); got != 2*n {
		t.Fatalf("unexpected sink result count: %d", got)
	}
	sum := 0
	for _, v := range res["sink"] {
		sum += v
	}
	want := n * (n + 2)
	if sum != want {
		t.Fatalf("unexpected sink sum: got %d want %d", sum, want)
	}
}
