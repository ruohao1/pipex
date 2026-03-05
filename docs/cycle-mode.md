# Cycle Mode

Cycle mode allows `Run` to execute pipelines that contain graph cycles.

By default, `Run` requires a DAG and returns `ErrCycle` for cyclic graphs.
Enable cycle mode only when you need recursive traversal behavior.

## Enable Cycle Mode

Use `WithCycleMode(maxHops, maxJobs, dedupKey)`:

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"a": {1}},
	pipex.WithCycleMode[int](3, 1000, nil),
)
```

Parameters:

- `maxHops`: maximum propagation depth from an ingress item.
  - `-1` means unlimited depth.
  - `0` means only ingress stage execution (no downstream propagation).
- `maxJobs`: maximum accepted jobs in one run.
  - must be `> 0` when cycle mode is enabled.
- `dedupKey`: optional run-local dedup key function.
  - Deprecated for new configuration. Prefer `WithDedupRules(...)`.
  - `nil` disables cycle-configured dedup.
  - when set, jobs with same `(stage, dedupKey(item))` are dropped after first acceptance.
  - remains supported for backward compatibility.

## Guardrails

Cycle mode uses three guardrails to prevent runaway recursion:

1. Hop limit (`maxHops`)
- Drops propagation once hop budget is exceeded.

2. Job budget (`maxJobs`)
- Returns `ErrCycleModeMaxJobsExceeded` when accepted job count exceeds budget.

3. Optional dedup (`dedupKey` or `WithDedupRules(...)`)
- Prevents revisiting same stage/key pair in one run.

## Cycle Hook Events

When hooks are configured with `WithHooks(...)`, cycle guardrails can emit:

- `CycleHopLimitDrop`
  - emitted when enqueue is skipped because `hops > maxHops`.
- `CycleDedupDrop`
  - emitted when enqueue is skipped because `(stage, dedupKey(item))` is already seen.
- `CycleMaxJobsExceeded`
  - emitted when enqueue is rejected because max-jobs budget is exhausted.

These events are enqueue/frontier signals (not stage-processing events).

## Validation Behavior

- `Run` validation:
  - allows cycles only when cycle mode is enabled.
- `Validate()`:
  - remains DAG-only and still reports `ErrCycle` for cyclic graphs.

## Error Cases

- `ErrCycleModeInvalidMaxHops` if `maxHops < -1`.
- `ErrCycleModeInvalidMaxJobs` if `maxJobs <= 0`.
- `ErrCycleModeMaxJobsExceeded` if job budget is exceeded.

## Dedup Migration (Recommended)

New dedup configuration should use `WithDedupRules(...)`.

Old style (still supported):

```go
pipex.WithCycleMode[int](-1, 100, func(v int) string { return fmt.Sprintf("%d", v) })
```

Recommended style:

```go
pipex.WithCycleMode[int](-1, 100, nil)
pipex.WithDedupRules[int](
	pipex.DedupRule[int]{
		Name:  "cycle-item",
		Scope: pipex.DedupScopeGlobal,
		Key: func(v int) string {
			return fmt.Sprintf("%d", v)
		},
	},
)
```

Both use the same runtime dedup path.

## Example: Cyclic Graph with Dedup

```go
_ = p.Connect("a", "b")
_ = p.Connect("b", "a")

res, err := p.Run(
	context.Background(),
	map[string][]int{"a": {1}},
	pipex.WithCycleMode[int](
		-1,   // unlimited hops
		100,  // max jobs
		func(v int) string { return fmt.Sprintf("%d", v) }, // dedup key
	),
)
```

This setup allows cycle traversal but prevents infinite revisits of the same item key per stage.
