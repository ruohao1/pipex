# Triggers

Triggers are runtime item sources for a specific stage.

- Configure with `WithTriggers(...)`.
- Each trigger emits items through `emit(item)`.
- Emitted items are enqueued the same way as seed inputs.

## Trigger Interface

```go
type Trigger[T any] interface {
	Name() string
	Stage() string
	Start(ctx context.Context, emit func(T) error) error
}
```

## Create a Trigger

Use `NewTrigger(...)` with `TriggerFunc`:

```go
tr := pipex.NewTrigger(
	"ticker",
	"src",
	pipex.TriggerFunc[int](func(ctx context.Context, emit func(int) error) error {
		for _, v := range []int{10, 20, 30} {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := emit(v); err != nil {
					return err
				}
			}
		}
		return nil
	}),
)
```

Run with trigger:

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2}},
	pipex.WithTriggers[int](tr),
)
```

## Lifecycle

- `Run` starts trigger goroutines during execution.
- `Run` waits for all triggers to return from `Start(...)`.
- A trigger that never returns keeps `Run` alive until context cancellation.

## Errors and Cancellation

- Trigger errors are included in `Run` error output.
- With `WithFailFast(true)`, first recorded error cancels the run context.
- If trigger code observes cancellation and returns `ctx.Err()`, that context error is handled by normal run semantics.

## Best Practices

- Always check `ctx.Done()` in long-running trigger loops.
- Keep emit loops non-blocking where possible.
- Use stable trigger names for observability and debugging.
