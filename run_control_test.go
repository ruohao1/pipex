package pipex

import (
	"errors"
	"testing"

	iruntime "github.com/ruohao1/pipex/internal/runtime"
)

func TestRunControlE_NotFound(t *testing.T) {
	if changed, err := PauseRunE("missing"); !errors.Is(err, ErrRunNotFound) || changed {
		t.Fatalf("pause missing got changed=%v err=%v", changed, err)
	}
	if changed, err := ResumeRunE("missing"); !errors.Is(err, ErrRunNotFound) || changed {
		t.Fatalf("resume missing got changed=%v err=%v", changed, err)
	}
	if changed, err := CancelRunE("missing"); !errors.Is(err, ErrRunNotFound) || changed {
		t.Fatalf("cancel missing got changed=%v err=%v", changed, err)
	}
}

func TestRunControlE_IdempotentNoop(t *testing.T) {
	h := iruntime.NewRunHandle("control-idempotent", nil)
	iruntime.RegisterRunHandle("control-idempotent", h)
	defer iruntime.UnregisterRunHandle("control-idempotent")

	changed, err := PauseRunE("control-idempotent")
	if err != nil || !changed {
		t.Fatalf("first pause got changed=%v err=%v", changed, err)
	}

	changed, err = PauseRunE("control-idempotent")
	if err != nil {
		t.Fatalf("second pause unexpected err=%v", err)
	}
	if changed {
		t.Fatal("second pause should be idempotent no-op")
	}
}
