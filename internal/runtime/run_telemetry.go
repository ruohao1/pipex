package runtime

import "time"

type RunTelemetry struct {
	RunID             string
	PauseCount        int64
	ResumeCount       int64
	TotalPaused       time.Duration
	CurrentlyPaused   bool
	CurrentPauseStart time.Time
	LastState         RunState
	UpdatedAt         time.Time
}

func RunTelemetryByID(runID string) (RunTelemetry, bool) {
	st, ok := RunStatusByID(runID)
	if !ok {
		return RunTelemetry{}, false
	}
	return runStatusToTelemetry(st), true
}

// RunTelemetrySnapshot returns a copy of run telemetry keyed by run_id.
// When activeOnly is true, terminal runs (completed/canceled) are excluded.
func RunTelemetrySnapshot(activeOnly bool) map[string]RunTelemetry {
	out := make(map[string]RunTelemetry)
	runHandleRegistry.Range(func(key, value any) bool {
		runID, ok := key.(string)
		if !ok {
			return true
		}
		h, ok := value.(*RunHandle)
		if !ok {
			return true
		}
		st := h.Status()
		if activeOnly && isTerminalRunState(st.State) {
			return true
		}
		out[runID] = runStatusToTelemetry(st)
		return true
	})
	return out
}

func runStatusToTelemetry(st RunStatus) RunTelemetry {
	return RunTelemetry{
		RunID:             st.RunID,
		PauseCount:        st.PauseCount,
		ResumeCount:       st.ResumeCount,
		TotalPaused:       st.TotalPaused,
		CurrentlyPaused:   st.CurrentlyPaused,
		CurrentPauseStart: st.CurrentPauseStart,
		LastState:         st.State,
		UpdatedAt:         st.UpdatedAt,
	}
}

func isTerminalRunState(state RunState) bool {
	return state == RunStateCanceled || state == RunStateCompleted
}
