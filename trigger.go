package pipex

import "context"

type Trigger[T any] interface {
	Name() string
	Stage() string
	Start(ctx context.Context, emit func(T) error) error
}

type TriggerFunc[T any] func(ctx context.Context, emit func(T) error) error

func (f TriggerFunc[T]) Start(ctx context.Context, emit func(T) error) error {
	return f(ctx, emit)
}

type stageTrigger[T any] struct {
	name  string
	stage string
	start TriggerFunc[T]
}

func (t stageTrigger[T]) Name() string { return t.name }

func (t stageTrigger[T]) Stage() string { return t.stage }

func (t stageTrigger[T]) Start(ctx context.Context, emit func(T) error) error {
	return t.start(ctx, emit)
}

// NewTrigger binds a TriggerFunc to a stage with a stable trigger name.
func NewTrigger[T any](name, stage string, fn TriggerFunc[T]) Trigger[T] {
	return stageTrigger[T]{
		name:  name,
		stage: stage,
		start: fn,
	}
}
