package runtime

import "sync"

var runHandleRegistry sync.Map // map[string]*RunHandle

func RegisterRunHandle(runID string, h *RunHandle) {
	if runID == "" || h == nil {
		return
	}
	runHandleRegistry.Store(runID, h)
}

func UnregisterRunHandle(runID string) {
	if runID == "" {
		return
	}
	runHandleRegistry.Delete(runID)
}

func PauseRun(runID string) bool {
	h, ok := lookupRunHandle(runID)
	if !ok {
		return false
	}
	return h.Pause()
}

func ResumeRun(runID string) bool {
	h, ok := lookupRunHandle(runID)
	if !ok {
		return false
	}
	return h.Resume()
}

func CancelRun(runID string) bool {
	h, ok := lookupRunHandle(runID)
	if !ok {
		return false
	}
	return h.Cancel()
}

func RunStatusByID(runID string) (RunStatus, bool) {
	h, ok := lookupRunHandle(runID)
	if !ok {
		return RunStatus{}, false
	}
	return h.Status(), true
}

func lookupRunHandle(runID string) (*RunHandle, bool) {
	v, ok := runHandleRegistry.Load(runID)
	if !ok {
		return nil, false
	}
	h, ok := v.(*RunHandle)
	return h, ok
}
