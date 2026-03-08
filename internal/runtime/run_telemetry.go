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
	return RunTelemetry{
		RunID:             st.RunID,
		PauseCount:        st.PauseCount,
		ResumeCount:       st.ResumeCount,
		TotalPaused:       st.TotalPaused,
		CurrentlyPaused:   st.CurrentlyPaused,
		CurrentPauseStart: st.CurrentPauseStart,
		LastState:         st.State,
		UpdatedAt:         st.UpdatedAt,
	}, true
}
