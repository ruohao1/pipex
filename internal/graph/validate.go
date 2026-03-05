package graph

import "slices"

type ValidationErrorKind int

const (
	ValidationNoStages ValidationErrorKind = iota + 1
	ValidationStageNotFound
	ValidationCycle
)

type ValidationError struct {
	Kind  ValidationErrorKind
	Stage string
}

func (e *ValidationError) Error() string {
	switch e.Kind {
	case ValidationNoStages:
		return "pipeline must have at least one stage"
	case ValidationStageNotFound:
		return "stage not found: " + e.Stage
	case ValidationCycle:
		return "pipeline graph contains a cycle"
	default:
		return "graph validation error"
	}
}

// ValidateSnapshot validates stage/edge consistency and cycle constraints.
func ValidateSnapshot(stages map[string]struct{}, edges map[string][]string) error {
	return ValidateSnapshotWithMode(stages, edges, false)
}

// ValidateSnapshotWithMode validates stage/edge consistency and can optionally
// allow cycles.
func ValidateSnapshotWithMode(stages map[string]struct{}, edges map[string][]string, allowCycles bool) error {
	if len(stages) == 0 {
		return &ValidationError{Kind: ValidationNoStages}
	}

	for from, tos := range edges {
		if _, ok := stages[from]; !ok {
			return &ValidationError{Kind: ValidationStageNotFound, Stage: from}
		}
		for _, to := range tos {
			if _, ok := stages[to]; !ok {
				return &ValidationError{Kind: ValidationStageNotFound, Stage: to}
			}
		}
	}
	if allowCycles {
		return nil
	}

	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(string) bool
	dfs = func(node string) bool {
		if recStack[node] {
			return true
		}
		if visited[node] {
			return false
		}

		visited[node] = true
		recStack[node] = true

		if slices.ContainsFunc(edges[node], dfs) {
			return true
		}

		recStack[node] = false
		return false
	}

	for stage := range stages {
		if !visited[stage] && dfs(stage) {
			return &ValidationError{Kind: ValidationCycle}
		}
	}

	return nil
}
