# v1 Migration Guide (Draft)

This guide tracks upgrade changes for `pipex` v1.

## Planned Breaking Changes

1. Remove `CycleModeOptions.DedupKey`.
2. Change `WithCycleMode(maxHops, maxJobs, dedupKey)` to `WithCycleMode(maxHops, maxJobs)`.
3. Remove cycle dedup compatibility shim from runtime.
4. Remove `CycleDedupDrop` hook and standardize on `DedupDrop`.

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

## Validation Checklist

- [ ] Build compiles with new `WithCycleMode` signature.
- [ ] Dedup behavior preserved using `WithDedupRules(...)`.
- [ ] Hook consumers migrated from `CycleDedupDrop` to `DedupDrop`.
- [ ] Test suite and race tests pass.
