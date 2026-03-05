# Stage Workers

`WithStageWorkers(map[string]int)` overrides stage worker counts for a single `Run`.

## Purpose

Use this option when you want to tune concurrency per stage at runtime without mutating stage definitions.

## Semantics

- Overrides are applied per stage name for that run only.
- Override values must be `> 0`.
- Override stage names must exist in the pipeline.
- `WithStageWorkers(...)` takes precedence over `Stage.Workers()`.
- If `WithStageWorkers(...)` is passed multiple times in one `Run`, the last call wins (replace semantics).

## Error Behavior

`Run` fails before processing starts when:

- an override stage name does not exist, or
- an override worker value is non-positive.

## Example

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"discover": seeds},
	pipex.WithStageWorkers[int](map[string]int{
		"discover": 32,
		"http":     128,
		"content":  64,
	}),
)
```

## Scope Boundary

- `WithStageWorkers(...)` controls global stage-level concurrency.
- It does not implement domain-specific policies like per-host throttling.
