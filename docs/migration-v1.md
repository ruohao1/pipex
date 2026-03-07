# v1 Migration Guide

This guide documents upgrade changes introduced in `pipex` v1 and runtime guidance aligned with the current implementation.

## v1 Breaking Changes (Completed)

1. Removed `CycleModeOptions.DedupKey`.
2. Changed `WithCycleMode(maxHops, maxJobs, dedupKey)` to `WithCycleMode(maxHops, maxJobs)`.
3. Removed cycle dedup compatibility shim from runtime.
4. Removed `CycleDedupDrop` hook and standardized on `DedupDrop`.

## Migration Summary

- Dedup moves to one API: `WithDedupRules(...)`.
- Cycle mode remains responsible for hop/job guardrails only.

## API Before/After

Before:

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"a": {1}},
	pipex.WithCycleMode[int](-1, 100, func(v int) string { return fmt.Sprintf("%d", v) }),
)
```

After:

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"a": {1}},
	pipex.WithCycleMode[int](-1, 100),
	pipex.WithDedupRules[int](
		pipex.DedupRule[int]{
			Name:  "cycle-item",
			Scope: pipex.DedupScopeGlobal, // or pipex.DedupScopeStage("stageName")
			Key: func(v int) string {
				return fmt.Sprintf("%d", v)
			},
		},
	),
)
```

## Hook Migration

Before:

- `CycleDedupDrop`

After:

- `DedupDrop`

If you relied on cycle-specific filtering, filter by dedup scope/stage in your hook handler.

## Runtime Mode Guidance (v1.1+)

`pipex` currently supports two runtime execution modes:

- `WithFrontier(true)`: frontier-backed recoverable scheduling mode (recommended for security workflows).
- `WithFrontier(false)`: ephemeral direct-dispatch mode (lower overhead baseline and fallback mode).

For long-running scan/crawl/enrichment workflows, prefer frontier mode. For small CPU-bound local transforms where throughput is the only goal, ephemeral mode may be preferable.

## Validation Checklist

- [x] Build compiles with new `WithCycleMode` signature.
- [x] Dedup behavior preserved using `WithDedupRules(...)`.
- [x] Hook consumers migrated from `CycleDedupDrop` to `DedupDrop`.
- [x] Test suite and race tests pass.
