package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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
	st := h.Status()
	if st.PauseCount != 1 || st.ResumeCount != 1 {
		t.Fatalf("pause/resume counts mismatch: %+v", st)
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

func TestRunHandle_PauseDurationTelemetry(t *testing.T) {
	h := NewRunHandle("r4", nil)
	base := time.Unix(1700000000, 0)
	now := base
	h.now = func() time.Time { return now }

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}
	now = now.Add(250 * time.Millisecond)
	if !h.Resume() {
		t.Fatal("resume should succeed")
	}

	st := h.Status()
	if st.TotalPaused != 250*time.Millisecond {
		t.Fatalf("total paused=%s want=%s", st.TotalPaused, 250*time.Millisecond)
	}
	if st.PauseCount != 1 || st.ResumeCount != 1 {
		t.Fatalf("unexpected counts: %+v", st)
	}
}

func TestRunHandle_PauseResumeHooks(t *testing.T) {
	h := NewRunHandle("r5", nil)
	var paused, resumed atomic.Int64
	h.SetHooks(RunHandleHooks{
		OnPause: func(st RunStatus) {
			paused.Add(1)
			if st.State != RunStatePaused {
				t.Fatalf("pause hook state=%s", st.State)
			}
		},
		OnResume: func(st RunStatus) {
			resumed.Add(1)
			if st.State != RunStateRunning {
				t.Fatalf("resume hook state=%s", st.State)
			}
		},
	})

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}
	if !h.Resume() {
		t.Fatal("resume should succeed")
	}
	if paused.Load() != 1 || resumed.Load() != 1 {
		t.Fatalf("unexpected hook counts paused=%d resumed=%d", paused.Load(), resumed.Load())
	}
}
