package runtime

import (
	"context"
	"sync"
	"time"
)

type RunState string

const (
	RunStateRunning   RunState = "running"
	RunStatePaused    RunState = "paused"
	RunStateCanceled  RunState = "canceled"
	RunStateCompleted RunState = "completed"
)

type RunStatus struct {
	RunID             string
	State             RunState
	UpdatedAt         time.Time
	PauseCount        int64
	ResumeCount       int64
	TotalPaused       time.Duration
	CurrentlyPaused   bool
	CurrentPauseStart time.Time
}

type RunHandleHooks struct {
	OnPause  func(RunStatus)
	OnResume func(RunStatus)
}

// RunHandle is an internal control surface for run lifecycle state.
// It is intentionally minimal and does not yet drive scheduler behavior.
type RunHandle struct {
	mu          sync.Mutex
	runID       string
	state       RunState
	now         func() time.Time
	cancel      context.CancelFunc
	pauseCount  int64
	resumeCount int64
	totalPaused time.Duration
	pausedSince time.Time
	hooks       RunHandleHooks

	hookEvents chan runHookEvent
	hookWG     sync.WaitGroup
	hookStop   sync.Once
}

func NewRunHandle(runID string, cancel context.CancelFunc) *RunHandle {
	h := &RunHandle{
		runID:  runID,
		state:  RunStateRunning,
		now:    time.Now,
		cancel: cancel,
		// Control transitions are low frequency; this buffer avoids blocking
		// most call paths while preserving FIFO hook order.
		hookEvents: make(chan runHookEvent, 64),
	}
	h.hookWG.Add(1)
	go func() {
		defer h.hookWG.Done()
		for ev := range h.hookEvents {
			h.dispatchHookEvent(ev)
		}
	}()
	return h
}

func (h *RunHandle) SetCancel(cancel context.CancelFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cancel = cancel
}

func (h *RunHandle) SetHooks(hooks RunHandleHooks) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hooks = hooks
}

// Pause transitions running -> paused. Returns true when state changed.
func (h *RunHandle) Pause() bool {
	h.mu.Lock()
	if h.state != RunStateRunning {
		h.mu.Unlock()
		return false
	}
	h.state = RunStatePaused
	h.pauseCount++
	h.pausedSince = h.now()
	snap := h.statusLocked(h.now())
	h.emitHookEventLocked(runHookEvent{
		kind: runHookPause,
		snap: snap,
	})
	h.mu.Unlock()
	return true
}

// Resume transitions paused -> running. Returns true when state changed.
func (h *RunHandle) Resume() bool {
	h.mu.Lock()
	if h.state != RunStatePaused {
		h.mu.Unlock()
		return false
	}
	if !h.pausedSince.IsZero() {
		h.totalPaused += h.now().Sub(h.pausedSince)
	}
	h.pausedSince = time.Time{}
	h.resumeCount++
	h.state = RunStateRunning
	snap := h.statusLocked(h.now())
	h.emitHookEventLocked(runHookEvent{
		kind: runHookResume,
		snap: snap,
	})
	h.mu.Unlock()
	return true
}

// Cancel transitions any non-terminal state -> canceled and calls cancel once.
// Returns true when state changed.
func (h *RunHandle) Cancel() bool {
	h.mu.Lock()
	changed := h.state != RunStateCanceled && h.state != RunStateCompleted
	if !changed {
		h.mu.Unlock()
		return false
	}
	if h.state == RunStatePaused && !h.pausedSince.IsZero() {
		h.totalPaused += h.now().Sub(h.pausedSince)
		h.pausedSince = time.Time{}
	}
	h.state = RunStateCanceled
	cancel := h.cancel
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	h.stopHookDispatcher()
	return true
}

// MarkCompleted transitions non-canceled state -> completed.
// Returns true when state changed.
func (h *RunHandle) MarkCompleted() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == RunStateCompleted || h.state == RunStateCanceled {
		return false
	}
	if h.state == RunStatePaused && !h.pausedSince.IsZero() {
		h.totalPaused += h.now().Sub(h.pausedSince)
		h.pausedSince = time.Time{}
	}
	h.state = RunStateCompleted
	go h.stopHookDispatcher()
	return true
}

func (h *RunHandle) Status() RunStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.statusLocked(h.now())
}

func (h *RunHandle) statusLocked(now time.Time) RunStatus {
	status := RunStatus{
		RunID:       h.runID,
		State:       h.state,
		UpdatedAt:   now,
		PauseCount:  h.pauseCount,
		ResumeCount: h.resumeCount,
		TotalPaused: h.totalPaused,
	}
	if h.state == RunStatePaused && !h.pausedSince.IsZero() {
		status.CurrentlyPaused = true
		status.CurrentPauseStart = h.pausedSince
	}
	return status
}

type runHookKind int

const (
	runHookPause runHookKind = iota
	runHookResume
)

type runHookEvent struct {
	kind runHookKind
	snap RunStatus
}

func (h *RunHandle) emitHookEventLocked(ev runHookEvent) {
	// Called with h.mu held so transition order maps to channel send order.
	h.hookEvents <- ev
}

func (h *RunHandle) dispatchHookEvent(ev runHookEvent) {
	h.mu.Lock()
	hooks := h.hooks
	h.mu.Unlock()

	switch ev.kind {
	case runHookPause:
		if hooks.OnPause != nil {
			hooks.OnPause(ev.snap)
		}
	case runHookResume:
		if hooks.OnResume != nil {
			hooks.OnResume(ev.snap)
		}
	}
}

func (h *RunHandle) stopHookDispatcher() {
	h.hookStop.Do(func() {
		close(h.hookEvents)
		h.hookWG.Wait()
	})
}
