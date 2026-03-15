package pipex

import "context"

type Stage[T any] interface {
	// Name returns the name of the stage.
	Name() string
	// Workers returns the number of workers for the stage.
	Workers() int

	// Process processes one input item and emits zero or more output items.
	//
	// Routing is handled by the pipeline graph (Connect edges), not by the stage.
	// Each emitted item is forwarded to all configured downstream stages.
	// Returning an error marks this item as failed and lets the pipeline apply
	// its configured error policy (for example, fail-fast or continue).
	Process(ctx context.Context, in T) (outs []T, err error)
}

// StreamingStage is an optional stage capability for incremental fanout.
//
// When a stage implements StreamingStage, the runtime can forward each emitted
// item to downstream stages immediately, without waiting for the full input item
// to finish processing.
//
// Retry policy semantics remain at-least-once for emitted outputs: if a
// streaming attempt emits items and then returns an error, a retry may emit
// duplicates.
type StreamingStage[T any] interface {
	Stage[T]
	ProcessStream(ctx context.Context, in T, emit func(T) error) error
}
