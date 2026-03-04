# pipex
A library to help you create pipelines in Golang

## Run Options

`Run` supports optional runtime behavior via `Option`:

- `WithBufferSize(n)`: per-stage channel buffer size (`n > 0`).
- `WithFailFast(v)`: when `true`, cancel execution on first stage error.
- `WithTriggers(...)`: register trigger sources that emit items during runtime.
- `WithPartialResults(v)`: when `true`, return currently collected results together with an error.

Defaults:

- `BufferSize`: `1024`
- `FailFast`: `false`

## Error Behavior

- Pipeline config/graph validation errors are returned before execution starts.
- If stage processing errors occur, `Run` returns a joined error (`errors.Join(...)`).
- When `FailFast=true`, the first stage error cancels the run, and the stage error is returned (not `context.Canceled`).
- If there are no stage errors but the context is canceled or times out, `Run` returns the context error.
- By default, errors return `nil` results.
- With `WithPartialResults(true)`, `Run` returns partial results plus the error.

## Trigger Lifecycle

- Triggers emit items through `emit(...)` into their configured stage, just like dynamic seeds.
- `Run` waits for all triggers to return from `Start(...)` before it can fully drain and close the pipeline.
- A non-terminating trigger keeps `Run` alive until context cancellation.

## Common Pitfalls

- Non-terminating triggers will keep `Run` alive. Always provide a cancellable context for long-lived triggers.
- If you need progress on failure/cancellation, enable `WithPartialResults(true)`.

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ruohao1/pipex"
)

type stageFn[T any] struct {
	name    string
	workers int
	fn      func(context.Context, T) ([]T, error)
}

func (s stageFn[T]) Name() string { return s.name }
func (s stageFn[T]) Workers() int { return s.workers }
func (s stageFn[T]) Process(ctx context.Context, in T) ([]T, error) {
	return s.fn(ctx, in)
}

func main() {
	p := pipex.NewPipeline[int]()

	src := stageFn[int]{
		name:    "src",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	}
	sink := stageFn[int]{
		name:    "sink",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	}

	_ = p.AddStage(src)
	_ = p.AddStage(sink)
	_ = p.Connect("src", "sink")

	tr := pipex.NewTrigger(
		"ticker",
		"src",
		pipex.TriggerFunc[int](func(ctx context.Context, emit func(int) error) error {
			for _, v := range []int{10, 20, 30} {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
					if err := emit(v); err != nil {
						return err
					}
				}
			}
			return nil
		}),
	)

	res, err := p.Run(
		context.Background(),
		map[string][]int{"src": {1, 2}}, // batch seeds
		pipex.WithBufferSize[int](64),
		pipex.WithFailFast[int](true),
		pipex.WithTriggers[int](tr), // streaming source
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("src outputs:", len(res["src"]))
	fmt.Println("sink outputs:", len(res["sink"]))
}
```

Run the example:

```bash
go run ./examples/quickstart
```
