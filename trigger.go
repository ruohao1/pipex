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
