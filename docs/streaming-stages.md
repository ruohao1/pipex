# Streaming Stages

`pipex` supports two stage execution styles:

- batch stages (`Stage`) return `[]T` after processing an input
- streaming stages (`StreamingStage`) emit items incrementally during processing

## Streaming Interface

```go
type StreamingStage[T any] interface {
	pipex.Stage[T]
	ProcessStream(ctx context.Context, in T, emit func(T) error) error
}
```

`emit(item)` forwards the item immediately through normal runtime flow:

- item is recorded in stage outputs
- item is delivered to stage sinks
- item is enqueued to downstream stages

This allows downstream stages to start work before the upstream stage finishes
processing the same input item.

## Retry Semantics

Streaming stages follow at-least-once emission semantics:

- if an attempt emits items and then returns an error
- and the stage policy retries the input
- emitted items can be emitted again on later attempts

Use `WithDedupRules(...)` when duplicate suppression is required.

## Hook Semantics

Stage hooks remain per input item:

- `StageStart`: emitted once before attempts
- `StageFinish`: emitted once on successful completion
- `StageError`: emitted once on terminal failure

`StageFinish.OutCount` reports the number of items emitted by the successful
attempt.

## Batch Compatibility

Existing `Stage` implementations continue to work unchanged. If a stage does
not implement `StreamingStage`, runtime behavior remains batch-oriented.

## Runnable Example

Run `go run ./examples/streaming` for a discovery-style pipeline that shows:

- downstream processing starts as soon as upstream emits each item
- retry behavior without dedup (duplicates visible)
- retry behavior with dedup (duplicates suppressed)
- retry + dedup behavior in both direct and frontier scheduling modes
