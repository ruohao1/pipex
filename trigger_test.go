package pipex

import (
	"context"
	"errors"
	"testing"
)

func TestNewTriggerMetadata(t *testing.T) {
	tr := NewTrigger(
		"events",
		"ingest",
		TriggerFunc[int](func(ctx context.Context, emit func(int) error) error {
			return nil
		}),
	)

	if got := tr.Name(); got != "events" {
		t.Fatalf("unexpected trigger name: got %q want %q", got, "events")
	}
	if got := tr.Stage(); got != "ingest" {
		t.Fatalf("unexpected trigger stage: got %q want %q", got, "ingest")
	}
}

func TestNewTriggerStartDelegates(t *testing.T) {
	called := false
	var emitted []int

	tr := NewTrigger(
		"events",
		"ingest",
		TriggerFunc[int](func(ctx context.Context, emit func(int) error) error {
			called = true
			_ = emit(7)
			_ = emit(8)
			return nil
		}),
	)

	err := tr.Start(context.Background(), func(v int) error {
		emitted = append(emitted, v)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}
	if !called {
		t.Fatal("expected wrapped trigger func to be called")
	}
	if len(emitted) != 2 || emitted[0] != 7 || emitted[1] != 8 {
		t.Fatalf("unexpected emitted values: %v", emitted)
	}
}

func TestTriggerFuncStartPropagatesError(t *testing.T) {
	want := errors.New("trigger error")
	tf := TriggerFunc[int](func(ctx context.Context, emit func(int) error) error {
		return want
	})

	err := tf.Start(context.Background(), func(v int) error { return nil })
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}
