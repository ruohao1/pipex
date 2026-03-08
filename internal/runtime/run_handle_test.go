package runtime

import (
	"context"
	"sync"
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
	done := make(chan struct{}, 2)
	h.SetHooks(RunHandleHooks{
		OnPause: func(st RunStatus) {
			paused.Add(1)
			if st.State != RunStatePaused {
				t.Fatalf("pause hook state=%s", st.State)
			}
			done <- struct{}{}
		},
		OnResume: func(st RunStatus) {
			resumed.Add(1)
			if st.State != RunStateRunning {
				t.Fatalf("resume hook state=%s", st.State)
			}
			done <- struct{}{}
		},
	})

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}
	if !h.Resume() {
		t.Fatal("resume should succeed")
	}
	waitForN(t, done, 2, time.Second)
	if paused.Load() != 1 || resumed.Load() != 1 {
		t.Fatalf("unexpected hook counts paused=%d resumed=%d", paused.Load(), resumed.Load())
	}
}

func TestRunHandle_HookOrderConcurrentPauseResume(t *testing.T) {
	h := NewRunHandle("r6", nil)
	var (
		mu     sync.Mutex
		events []RunState
	)
	done := make(chan struct{}, 2)
	h.SetHooks(RunHandleHooks{
		OnPause: func(st RunStatus) {
			mu.Lock()
			events = append(events, st.State)
			mu.Unlock()
			done <- struct{}{}
		},
		OnResume: func(st RunStatus) {
			mu.Lock()
			events = append(events, st.State)
			mu.Unlock()
			done <- struct{}{}
		},
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = h.Pause()
	}()
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(200 * time.Millisecond)
		for !h.Resume() && time.Now().Before(deadline) {
			time.Sleep(1 * time.Millisecond)
		}
	}()
	wg.Wait()
	waitForN(t, done, 2, time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d (%v)", len(events), events)
	}
	if events[0] != RunStatePaused || events[1] != RunStateRunning {
		t.Fatalf("unexpected hook order: %v", events)
	}
}

func waitForN(t *testing.T, ch <-chan struct{}, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("timeout waiting for %d events (received %d)", n, i)
		}
	}
}
