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

	ErrEdgeExists = errors.New("edge already exists")
	ErrCycle      = errors.New("pipeline graph contains a cycle")

	ErrNotEnoughStages = errors.New("not enough stages to connect")
)

func StageNotFound(name string) error {
	return fmt.Errorf("%w: %s", ErrStageNotFound, name)
}
