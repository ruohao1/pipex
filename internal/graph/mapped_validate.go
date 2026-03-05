package graph

import "errors"

// ValidateSnapshotMapped validates graph snapshot and maps internal validation
// errors to caller-provided errors.
func ValidateSnapshotMapped(
	stages map[string]struct{},
	edges map[string][]string,
	allowCycles bool,
	noStages func() error,
	stageNotFound func(string) error,
	cycle func() error,
) error {
	err := ValidateSnapshotWithMode(stages, edges, allowCycles)
	if err == nil {
		return nil
	}

	var vErr *ValidationError
	if errors.As(err, &vErr) {
		switch vErr.Kind {
		case ValidationNoStages:
			return noStages()
		case ValidationStageNotFound:
			return stageNotFound(vErr.Stage)
		case ValidationCycle:
			return cycle()
		}
	}

	return err
}
