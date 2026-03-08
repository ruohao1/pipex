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
	RunID     string
	State     RunState
	UpdatedAt time.Time
}

// RunHandle is an internal control surface for run lifecycle state.
// It is intentionally minimal and does not yet drive scheduler behavior.
type RunHandle struct {
	mu     sync.Mutex
	runID  string
	state  RunState
	now    func() time.Time
	cancel context.CancelFunc
}

func NewRunHandle(runID string, cancel context.CancelFunc) *RunHandle {
	return &RunHandle{
		runID:  runID,
		state:  RunStateRunning,
		now:    time.Now,
		cancel: cancel,
	}
}

// Pause transitions running -> paused. Returns true when state changed.
func (h *RunHandle) Pause() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != RunStateRunning {
		return false
	}
	h.state = RunStatePaused
	return true
}

// Resume transitions paused -> running. Returns true when state changed.
func (h *RunHandle) Resume() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != RunStatePaused {
		return false
	}
	h.state = RunStateRunning
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
	h.state = RunStateCompleted
	return true
}

func (h *RunHandle) Status() RunStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return RunStatus{
		RunID:     h.runID,
		State:     h.state,
		UpdatedAt: h.now(),
	}
}
