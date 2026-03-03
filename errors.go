package pipex

import (
	"errors"
	"fmt"
)

var (
	ErrNoStages = errors.New("pipeline must have at least one stage")
	ErrNilStage = errors.New("stage cannot be nil")
	ErrStageNameEmpty = errors.New("stage name cannot be empty")
	ErrStageNotFound = func(name string) error {
		return errors.New("stage not found: " + name)
	}
	ErrStageExists = func(name string) error {
		return errors.New("stage already exists: " + name)
	}
 ErrStageInvalidWorkerCount = func(name string, count int) error {
  	return fmt.Errorf("invalid worker count for stage %s: %d", name, count)
  }

	ErrEdgeExists = func(from, to string) error {
		return errors.New("edge already exists: " + from + " -> " + to)
	}
	ErrCycle    = errors.New("pipeline graph contains a cycle")
)
