package pipex

import iruntime "github.com/ruohao1/pipex/internal/runtime"

// PauseRun pauses a running pipeline by run_id. It returns true when state changed.
func PauseRun(runID string) bool {
	changed, _ := PauseRunE(runID)
	return changed
}

// ResumeRun resumes a paused pipeline by run_id. It returns true when state changed.
func ResumeRun(runID string) bool {
	changed, _ := ResumeRunE(runID)
	return changed
}

// CancelRun cancels a pipeline by run_id. It returns true when state changed.
func CancelRun(runID string) bool {
	changed, _ := CancelRunE(runID)
	return changed
}

// PauseRunE pauses a running pipeline by run_id.
// It returns ErrRunNotFound when run_id is unknown.
func PauseRunE(runID string) (bool, error) {
	if _, ok := iruntime.RunStatusByID(runID); !ok {
		return false, ErrRunNotFound
	}
	return iruntime.PauseRun(runID), nil
}

// ResumeRunE resumes a paused pipeline by run_id.
// It returns ErrRunNotFound when run_id is unknown.
func ResumeRunE(runID string) (bool, error) {
	if _, ok := iruntime.RunStatusByID(runID); !ok {
		return false, ErrRunNotFound
	}
	return iruntime.ResumeRun(runID), nil
}

// CancelRunE cancels a pipeline by run_id.
// It returns ErrRunNotFound when run_id is unknown.
func CancelRunE(runID string) (bool, error) {
	if _, ok := iruntime.RunStatusByID(runID); !ok {
		return false, ErrRunNotFound
	}
	return iruntime.CancelRun(runID), nil
}
