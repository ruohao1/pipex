package runtime

import "testing"

func TestRunRegistryPauseResumeAndStatus(t *testing.T) {
	h := NewRunHandle("rid", nil)
	RegisterRunHandle("rid", h)
	defer UnregisterRunHandle("rid")

	if !PauseRun("rid") {
		t.Fatal("expected pause to succeed")
	}
	st, ok := RunStatusByID("rid")
	if !ok {
		t.Fatal("expected status to be available")
	}
	if st.State != RunStatePaused {
		t.Fatalf("state=%s want=%s", st.State, RunStatePaused)
	}

	if !ResumeRun("rid") {
		t.Fatal("expected resume to succeed")
	}
	st, ok = RunStatusByID("rid")
	if !ok {
		t.Fatal("expected status to be available")
	}
	if st.State != RunStateRunning {
		t.Fatalf("state=%s want=%s", st.State, RunStateRunning)
	}
}

func TestRunRegistryUnknownRunIDNoop(t *testing.T) {
	if PauseRun("missing") {
		t.Fatal("expected pause missing to be false")
	}
	if ResumeRun("missing") {
		t.Fatal("expected resume missing to be false")
	}
	if CancelRun("missing") {
		t.Fatal("expected cancel missing to be false")
	}
	if _, ok := RunStatusByID("missing"); ok {
		t.Fatal("expected missing status to be absent")
	}
}
