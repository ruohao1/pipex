package runtime

import (
	"testing"
	"time"
)

func TestRunTelemetryByID_Present(t *testing.T) {
	h := NewRunHandle("telemetry-run", nil)
	base := time.Unix(1700001000, 0)
	now := base
	h.now = func() time.Time { return now }
	RegisterRunHandle("telemetry-run", h)
	defer UnregisterRunHandle("telemetry-run")

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}
	now = now.Add(100 * time.Millisecond)
	if !h.Resume() {
		t.Fatal("resume should succeed")
	}

	tm, ok := RunTelemetryByID("telemetry-run")
	if !ok {
		t.Fatal("expected telemetry to exist")
	}
	if tm.RunID != "telemetry-run" {
		t.Fatalf("run id=%s", tm.RunID)
	}
	if tm.PauseCount != 1 || tm.ResumeCount != 1 {
		t.Fatalf("unexpected counts: %+v", tm)
	}
	if tm.TotalPaused != 100*time.Millisecond {
		t.Fatalf("total paused=%s want=%s", tm.TotalPaused, 100*time.Millisecond)
	}
	if tm.CurrentlyPaused {
		t.Fatal("expected currently paused=false after resume")
	}
	if tm.LastState != RunStateRunning {
		t.Fatalf("last state=%s want=%s", tm.LastState, RunStateRunning)
	}
}

func TestRunTelemetryByID_Missing(t *testing.T) {
	_, ok := RunTelemetryByID("missing-run")
	if ok {
		t.Fatal("expected missing telemetry lookup to fail")
	}
}

func TestRunTelemetryByID_CurrentlyPausedFields(t *testing.T) {
	h := NewRunHandle("paused-run", nil)
	base := time.Unix(1700002000, 0)
	now := base
	h.now = func() time.Time { return now }
	RegisterRunHandle("paused-run", h)
	defer UnregisterRunHandle("paused-run")

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}
	tm, ok := RunTelemetryByID("paused-run")
	if !ok {
		t.Fatal("expected telemetry to exist")
	}
	if !tm.CurrentlyPaused {
		t.Fatal("expected currently paused=true")
	}
	if tm.CurrentPauseStart.IsZero() {
		t.Fatal("expected current pause start to be set")
	}
	if tm.LastState != RunStatePaused {
		t.Fatalf("last state=%s want=%s", tm.LastState, RunStatePaused)
	}
}
