# Rate Limits

`WithStageRateLimits(map[string]RateLimit)` applies per-stage, per-run rate limiting.

## Purpose

Use this option to control how quickly stage workers start processing items without changing stage definitions.

## Type

```go
type RateLimit struct {
	RPS   float64
	Burst int
}
```

## Semantics

- Limits are keyed by stage name and apply only to that `Run`.
- A stage limiter is shared by all workers in that stage (global stage rate).
- `RateLimit` is enforced before each `Stage.Process(...)`.
- Multiple `WithStageRateLimits(...)` calls in one `Run` use last-call-wins replacement semantics.

## Validation

`Run` fails before processing starts when:

- a configured stage name does not exist,
- `RPS <= 0`, or
- `Burst < 1`.

## Example

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"discover": seeds},
	pipex.WithStageRateLimits[int](map[string]pipex.RateLimit{
		"http_probe": {RPS: 200, Burst: 50},
		"content":    {RPS: 120, Burst: 20},
	}),
)
```

## Scope Boundary

- Stage rate limits are global per stage.
- Per-host/domain throttling remains stage/business logic, not a pipeline option.

## Interaction With Stage Workers

- `WithStageWorkers(...)` controls concurrent in-flight work per stage.
- `WithStageRateLimits(...)` controls start rate (items/sec) per stage.
- Combining both gives bounded parallelism plus paced execution.
