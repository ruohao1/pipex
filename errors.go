package pipex

import (
	"errors"
	"fmt"
)

var (
	ErrNoStages       = errors.New("pipeline must have at least one stage")
	ErrNilStage       = errors.New("stage cannot be nil")
	ErrStageNameEmpty = errors.New("stage name cannot be empty")
	ErrStageNotFound  = errors.New("stage not found")
	ErrStageExists    = errors.New("stage already exists")
	ErrStageConflict  = errors.New("stage definition conflict")

	ErrStageInvalidWorkerCount = func(name string, count int) error {
		return fmt.Errorf("invalid worker count for stage %s: %d", name, count)
	}

	ErrEdgeExists               = errors.New("edge already exists")
	ErrCycle                    = errors.New("pipeline graph contains a cycle")
	ErrCycleModeInvalidMaxHops  = errors.New("cycle mode max hops must be -1 or greater")
	ErrCycleModeInvalidMaxJobs  = errors.New("cycle mode max jobs must be greater than zero")
	ErrCycleModeMaxJobsExceeded = errors.New("cycle mode max jobs exceeded")

	ErrNotEnoughStages = errors.New("not enough stages to connect")

	ErrInvalidWorkerCount = errors.New("worker count must be greater than zero")
	ErrInvalidQueueSize   = errors.New("queue size must be non-negative")
	ErrNilJobFunc         = errors.New("job function cannot be nil")

	ErrInvalidRPS   = errors.New("RPS must be greater than zero")
	ErrInvalidBurst = errors.New("Burst must be greater than zero")

	ErrInvalidDedupRuleName  = errors.New("dedup rule name cannot be empty")
	ErrInvalidDedupRuleScope = errors.New("dedup rule scope cannot be empty")
	ErrInvalidDedupRuleKey   = errors.New("dedup rule key function cannot be nil")
)

func StageNotFound(name string) error {
	return fmt.Errorf("%w: %s", ErrStageNotFound, name)
}

func CycleModeMaxJobsExceeded(maxJobs int) error {
	return fmt.Errorf("%w: %d", ErrCycleModeMaxJobsExceeded, maxJobs)
}
