package pipex

import "context"

type Sink[T any] interface {
	// Name returns the name of the sink.
	Name() string
	// Stage returns the name of the stage that this sink is associated with.
	Stage() string
	// Consume processes one item and returns an error if processing fails.
	Consume(ctx context.Context, item T) error
}
