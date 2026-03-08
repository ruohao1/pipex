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
}

func NewRunHandle(runID string, cancel context.CancelFunc) *RunHandle {
	return &RunHandle{
		runID:  runID,
		state:  RunStateRunning,
		now:    time.Now,
		cancel: cancel,
	}
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
	onPause := h.hooks.OnPause
	h.mu.Unlock()
	if onPause != nil {
		onPause(snap)
	}
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
	onResume := h.hooks.OnResume
	h.mu.Unlock()
	if onResume != nil {
		onResume(snap)
	}
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
