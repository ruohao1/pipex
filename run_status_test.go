package pipex

import (
	"testing"

	iruntime "github.com/ruohao1/pipex/internal/runtime"
)

func TestGetRunStatus_Present(t *testing.T) {
	h := iruntime.NewRunHandle("status-run", nil)
	iruntime.RegisterRunHandle("status-run", h)
	defer iruntime.UnregisterRunHandle("status-run")

	if !h.Pause() {
		t.Fatal("pause should succeed")
	}

	st, ok := GetRunStatus("status-run")
	if !ok {
		t.Fatal("expected status to exist")
	}
	if st.RunID != "status-run" {
		t.Fatalf("run id=%s", st.RunID)
	}
	if st.State != RunStatePaused {
		t.Fatalf("state=%s want=%s", st.State, RunStatePaused)
	}
	if st.UpdatedAt.IsZero() {
		t.Fatal("updated_at should be set")
	}
}

func TestGetRunStatus_Missing(t *testing.T) {
	if _, ok := GetRunStatus("missing-run"); ok {
		t.Fatal("expected missing status to be absent")
	}
}
