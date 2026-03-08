package pipex

import (
	"time"

	iruntime "github.com/ruohao1/pipex/internal/runtime"
)

// RunStatus is a lightweight run status snapshot for frequent polling.
type RunStatus struct {
	RunID     string
	State     RunState
	UpdatedAt time.Time
}

// GetRunStatus returns run status for a run_id when it is currently tracked.
func GetRunStatus(runID string) (RunStatus, bool) {
	st, ok := iruntime.RunStatusByID(runID)
	if !ok {
		return RunStatus{}, false
	}
	return RunStatus{
		RunID:     st.RunID,
		State:     RunState(st.State),
		UpdatedAt: st.UpdatedAt,
	}, true
}
