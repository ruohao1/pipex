package pipex

import (
	"time"

	iruntime "github.com/ruohao1/pipex/internal/runtime"
)

type RunState string

const (
	RunStateRunning   RunState = "running"
	RunStatePaused    RunState = "paused"
	RunStateCanceled  RunState = "canceled"
	RunStateCompleted RunState = "completed"
)

// RunTelemetry is a public snapshot of run lifecycle telemetry.
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

// GetRunTelemetry returns telemetry for a run_id when it is currently tracked.
func GetRunTelemetry(runID string) (RunTelemetry, bool) {
	tm, ok := iruntime.RunTelemetryByID(runID)
	if !ok {
		return RunTelemetry{}, false
	}
	return toPublicRunTelemetry(tm), true
}

// GetRunTelemetrySnapshot returns run telemetry keyed by run_id.
// When activeOnly is true, terminal runs are excluded.
func GetRunTelemetrySnapshot(activeOnly bool) map[string]RunTelemetry {
	internal := iruntime.RunTelemetrySnapshot(activeOnly)
	out := make(map[string]RunTelemetry, len(internal))
	for runID, tm := range internal {
		out[runID] = toPublicRunTelemetry(tm)
	}
	return out
}

func toPublicRunTelemetry(tm iruntime.RunTelemetry) RunTelemetry {
	return RunTelemetry{
		RunID:             tm.RunID,
		PauseCount:        tm.PauseCount,
		ResumeCount:       tm.ResumeCount,
		TotalPaused:       tm.TotalPaused,
		CurrentlyPaused:   tm.CurrentlyPaused,
		CurrentPauseStart: tm.CurrentPauseStart,
		LastState:         RunState(tm.LastState),
		UpdatedAt:         tm.UpdatedAt,
	}
}
