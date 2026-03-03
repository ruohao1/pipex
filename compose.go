package pipex

import (
	"errors"
	"fmt"
	"reflect"
)

func (p *Pipeline[T]) Sequence(stages ...Stage[T]) error {
	if len(stages) < 2 {
		return ErrNotEnoughStages
	}

	for i := range len(stages) - 1 {
		if stages[i] == nil {
			return fmt.Errorf("sequence stage %d: %w", i, ErrNilStage)
		}
		if err := p.addOrValidateStage(stages[i]); err != nil {
			return fmt.Errorf("sequence stage %d: %w", i, err)
		}

		if stages[i+1] == nil {
			return fmt.Errorf("sequence stage %d: %w", i+1, ErrNilStage)
		}
		if err := p.addOrValidateStage(stages[i+1]); err != nil {
			return fmt.Errorf("sequence stage %d: %w", i+1, err)
		}

		if err := p.Connect(stages[i].Name(), stages[i+1].Name()); err != nil && !errors.Is(err, ErrEdgeExists) {
			return err
		}
	}

	return nil
}

func (p *Pipeline[T]) Parallel(stages ...Stage[T]) error {
	if len(stages) == 0 {
		return nil
	}

	for i, stage := range stages {
		if stage == nil {
			return fmt.Errorf("parallel stage %d: %w", i, ErrNilStage)
		}
		if err := p.addOrValidateStage(stage); err != nil {
			return fmt.Errorf("parallel stage %d: %w", i, err)
		}
	}

	return nil
}

func (p *Pipeline[T]) Merge(ins []Stage[T], out Stage[T]) error {
	if len(ins) < 1 {
		return fmt.Errorf("merge inputs: %w", ErrNotEnoughStages)
	}
	if out == nil {
		return fmt.Errorf("merge output stage: %w", ErrNilStage)
	}
	if err := p.addOrValidateStage(out); err != nil {
		return fmt.Errorf("merge output stage: %w", err)
	}

	for i, stage := range ins {
		if stage == nil {
			return fmt.Errorf("merge input stage %d: %w", i, ErrNilStage)
		}
		if err := p.addOrValidateStage(stage); err != nil {
			return fmt.Errorf("merge input stage %d: %w", i, err)
		}
		if err := p.Connect(stage.Name(), out.Name()); err != nil && !errors.Is(err, ErrEdgeExists) {
			return err
		}
	}

	return nil
}

func (p *Pipeline[T]) Split(in Stage[T], outs ...Stage[T]) error {
	if in == nil {
		return fmt.Errorf("split input stage: %w", ErrNilStage)
	}
	if len(outs) < 1 {
		return fmt.Errorf("split outputs: %w", ErrNotEnoughStages)
	}
	if err := p.addOrValidateStage(in); err != nil {
		return fmt.Errorf("split input stage: %w", err)
	}

	for i, stage := range outs {
		if stage == nil {
			return fmt.Errorf("split output stage %d: %w", i, ErrNilStage)
		}
		if err := p.addOrValidateStage(stage); err != nil {
			return fmt.Errorf("split output stage %d: %w", i, err)
		}
		if err := p.Connect(in.Name(), stage.Name()); err != nil && !errors.Is(err, ErrEdgeExists) {
			return err
		}
	}

	return nil
}

func (p *Pipeline[T]) addOrValidateStage(stage Stage[T]) error {
	p.mu.RLock()
	existing, exists := p.stages[stage.Name()]
	p.mu.RUnlock()
	if exists {
		return stageCompatibilityError(existing, stage)
	}

	if err := p.AddStage(stage); err == nil {
		return nil
	} else if !errors.Is(err, ErrStageExists) {
		return err
	}

	// Stage may have been added concurrently; re-check compatibility.
	p.mu.RLock()
	existing, exists = p.stages[stage.Name()]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %s", ErrStageExists, stage.Name())
	}
	return stageCompatibilityError(existing, stage)
}

func stageCompatibilityError[T any](existing, candidate Stage[T]) error {
	if reflect.TypeOf(existing) != reflect.TypeOf(candidate) || existing.Workers() != candidate.Workers() {
		return fmt.Errorf(
			"%w: %s (existing type=%T workers=%d, candidate type=%T workers=%d)",
			ErrStageConflict,
			candidate.Name(),
			existing,
			existing.Workers(),
			candidate,
			candidate.Workers(),
		)
	}
	return nil
}
