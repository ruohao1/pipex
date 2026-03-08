package pipex

import iruntime "github.com/ruohao1/pipex/internal/runtime"

// PauseRun pauses a running pipeline by run_id. It returns true when state changed.
func PauseRun(runID string) bool {
	return iruntime.PauseRun(runID)
}

// ResumeRun resumes a paused pipeline by run_id. It returns true when state changed.
func ResumeRun(runID string) bool {
	return iruntime.ResumeRun(runID)
}

// CancelRun cancels a pipeline by run_id. It returns true when state changed.
func CancelRun(runID string) bool {
	return iruntime.CancelRun(runID)
}
