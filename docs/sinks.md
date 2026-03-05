# Sinks

Sinks consume per-item outputs from a stage.

- Configure with `WithSinks(...)`.
- A sink is attached to one stage (`Sink.Stage()`).
- Each output item is delivered via `Consume(ctx, item)`.

## Sink Interface

```go
type Sink[T any] interface {
	Name() string
	Stage() string
	Consume(ctx context.Context, item T) error
}
```

## Basic Usage

```go
type collectSink struct {
	name  string
	stage string
}

func (s *collectSink) Name() string  { return s.name }
func (s *collectSink) Stage() string { return s.stage }
func (s *collectSink) Consume(ctx context.Context, item int) error {
	// write item to DB/queue/file
	return nil
}

sink := &collectSink{name: "collector", stage: "out"}

_, err := p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2, 3}},
	pipex.WithSinks[int](sink),
)
```

## Retry Policy

Configure with `WithSinkRetry(maxRetries, backoff)`:

- `maxRetries = 0`: no retries after first failure.
- `maxRetries > 0`: bounded retries.
- `maxRetries = -1`: infinite retries.

Backoff:

- `backoff = 0` normalizes to `1ms`.
- `backoff < 0` is ignored.

## Failure Behavior

When a sink fails for an item:

1. Runtime retries based on sink retry policy.
2. If retry budget is exceeded:
   - sink error is recorded,
   - that sink is disabled for the rest of the run,
   - pipeline continues unless `WithFailFast(true)` is set.

With `WithFailFast(true)`:

- first recorded error cancels run context and returns early semantics.

## Cancellation

- Sink workers stop on `ctx.Done()`.
- `Consume` implementations should respect context cancellation promptly.

## Best Practices

- Make `Consume` idempotent when possible.
- Use bounded retries for most workloads.
- Keep sink work lightweight or hand off to durable downstream systems.
