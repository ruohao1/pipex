package pipex

import "context"

type Stage[T any] interface {
	// Name returns the name of the stage.
	Name() string
	// Workers returns the number of workers for the stage.
	Workers() int
	// Run processes one input item and emits zero or more output items.
  //
  // Routing is handled by the pipeline graph (Connect edges), not by the stage.
  // Each emitted item is forwarded to all configured downstream stages.
  // Returning an error marks this item as failed and lets the pipeline apply
  // its configured error policy (for example, fail-fast or continue).
	Run(ctx context.Context, in T) (out []T, err error)
}

