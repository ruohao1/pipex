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

func TestRunTelemetrySnapshot_AllRuns(t *testing.T) {
	running := NewRunHandle("snap-running", nil)
	paused := NewRunHandle("snap-paused", nil)
	completed := NewRunHandle("snap-completed", nil)
	RegisterRunHandle("snap-running", running)
	RegisterRunHandle("snap-paused", paused)
	RegisterRunHandle("snap-completed", completed)
	defer UnregisterRunHandle("snap-running")
	defer UnregisterRunHandle("snap-paused")
	defer UnregisterRunHandle("snap-completed")

	if !paused.Pause() {
		t.Fatal("pause should succeed")
	}
	if !completed.MarkCompleted() {
		t.Fatal("mark completed should succeed")
	}

	snap := RunTelemetrySnapshot(false)
	if len(snap) != 3 {
		t.Fatalf("snapshot size=%d want=3", len(snap))
	}
	if snap["snap-running"].LastState != RunStateRunning {
		t.Fatalf("running state=%s want=%s", snap["snap-running"].LastState, RunStateRunning)
	}
	if snap["snap-paused"].LastState != RunStatePaused {
		t.Fatalf("paused state=%s want=%s", snap["snap-paused"].LastState, RunStatePaused)
	}
	if snap["snap-completed"].LastState != RunStateCompleted {
		t.Fatalf("completed state=%s want=%s", snap["snap-completed"].LastState, RunStateCompleted)
	}
}

func TestRunTelemetrySnapshot_ActiveOnly(t *testing.T) {
	running := NewRunHandle("active-running", nil)
	paused := NewRunHandle("active-paused", nil)
	canceled := NewRunHandle("active-canceled", nil)
	RegisterRunHandle("active-running", running)
	RegisterRunHandle("active-paused", paused)
	RegisterRunHandle("active-canceled", canceled)
	defer UnregisterRunHandle("active-running")
	defer UnregisterRunHandle("active-paused")
	defer UnregisterRunHandle("active-canceled")

	if !paused.Pause() {
		t.Fatal("pause should succeed")
	}
	if !canceled.Cancel() {
		t.Fatal("cancel should succeed")
	}

	snap := RunTelemetrySnapshot(true)
	if _, ok := snap["active-canceled"]; ok {
		t.Fatal("expected canceled run to be excluded for activeOnly")
	}
	if _, ok := snap["active-running"]; !ok {
		t.Fatal("expected running run in activeOnly snapshot")
	}
	if _, ok := snap["active-paused"]; !ok {
		t.Fatal("expected paused run in activeOnly snapshot")
	}
}
