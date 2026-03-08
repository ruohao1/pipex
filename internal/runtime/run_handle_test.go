package runtime

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestRunHandle_PauseResumeIdempotent(t *testing.T) {
	h := NewRunHandle("r1", nil)

	if !h.Pause() {
		t.Fatal("expected first pause to change state")
	}
	if h.Pause() {
		t.Fatal("expected second pause to be no-op")
	}
	if got := h.Status().State; got != RunStatePaused {
		t.Fatalf("state=%s want=%s", got, RunStatePaused)
	}

	if !h.Resume() {
		t.Fatal("expected resume from paused to change state")
	}
	if h.Resume() {
		t.Fatal("expected second resume to be no-op")
	}
	if got := h.Status().State; got != RunStateRunning {
		t.Fatalf("state=%s want=%s", got, RunStateRunning)
	}
}

func TestRunHandle_CancelIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cancelCalls atomic.Int64
	h := NewRunHandle("r2", func() {
		cancelCalls.Add(1)
		cancel()
	})

	if !h.Cancel() {
		t.Fatal("expected first cancel to change state")
	}
	if h.Cancel() {
		t.Fatal("expected second cancel to be no-op")
	}
	if got := h.Status().State; got != RunStateCanceled {
		t.Fatalf("state=%s want=%s", got, RunStateCanceled)
	}
	if cancelCalls.Load() != 1 {
		t.Fatalf("cancel calls=%d want=1", cancelCalls.Load())
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("expected context cancellation")
	}
}

func TestRunHandle_CompletedTerminal(t *testing.T) {
	h := NewRunHandle("r3", nil)

	if !h.MarkCompleted() {
		t.Fatal("expected first completion to change state")
	}
	if h.MarkCompleted() {
		t.Fatal("expected second completion to be no-op")
	}
	if h.Cancel() {
		t.Fatal("expected cancel after completion to be no-op")
	}
	if got := h.Status().State; got != RunStateCompleted {
		t.Fatalf("state=%s want=%s", got, RunStateCompleted)
	}
}
