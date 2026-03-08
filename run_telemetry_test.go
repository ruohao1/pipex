package pipex

import (
	"testing"

	iruntime "github.com/ruohao1/pipex/internal/runtime"
)

func TestGetRunTelemetry_Present(t *testing.T) {
	h := iruntime.NewRunHandle("public-telemetry-run", nil)
	iruntime.RegisterRunHandle("public-telemetry-run", h)
	defer iruntime.UnregisterRunHandle("public-telemetry-run")

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}
	if !h.Resume() {
		t.Fatal("resume should succeed")
	}

	tm, ok := GetRunTelemetry("public-telemetry-run")
	if !ok {
		t.Fatal("expected telemetry to exist")
	}
	if tm.RunID != "public-telemetry-run" {
		t.Fatalf("run id=%s", tm.RunID)
	}
	if tm.PauseCount != 1 || tm.ResumeCount != 1 {
		t.Fatalf("unexpected pause/resume counts: %+v", tm)
	}
	if tm.CurrentlyPaused {
		t.Fatal("expected currently paused=false after resume")
	}
	if tm.LastState != "running" {
		t.Fatalf("last state=%s want=running", tm.LastState)
	}
}

func TestGetRunTelemetry_Missing(t *testing.T) {
	if _, ok := GetRunTelemetry("missing-run"); ok {
		t.Fatal("expected missing telemetry to be absent")
	}
}

func TestGetRunTelemetrySnapshot_ActiveOnly(t *testing.T) {
	running := iruntime.NewRunHandle("public-active-running", nil)
	canceled := iruntime.NewRunHandle("public-active-canceled", nil)
	iruntime.RegisterRunHandle("public-active-running", running)
	iruntime.RegisterRunHandle("public-active-canceled", canceled)
	defer iruntime.UnregisterRunHandle("public-active-running")
	defer iruntime.UnregisterRunHandle("public-active-canceled")

	if !canceled.Cancel() {
		t.Fatal("cancel should succeed")
	}

	snap := GetRunTelemetrySnapshot(true)
	if _, ok := snap["public-active-canceled"]; ok {
		t.Fatal("expected canceled run to be filtered in activeOnly snapshot")
	}
	if _, ok := snap["public-active-running"]; !ok {
		t.Fatal("expected running run to be included")
	}
}
