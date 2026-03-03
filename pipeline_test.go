package pipex

import (
	"context"
	"testing"
)

type testStage[T any] struct {
	name    string
	workers int
}

func (s testStage[T]) Name() string { return s.name }

func (s testStage[T]) Workers() int { return s.workers }

func (s testStage[T]) Run(ctx context.Context, in T) (out []T, err error) {
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
