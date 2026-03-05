# Product Plan

This document tracks feature planning, milestones, and active TODOs for `pipex`.

## Planning Goals

- Keep `pipex` simple for DAG-first workflows.
- Support scanner/crawler-style recursive workloads behind explicit opt-in controls.
- Evolve runtime safety and observability without breaking default semantics.

## Current Status

- Per-stage worker pools and run-level worker overrides are available.
- Trigger and sink runtime integrations are available.
- Cycle mode is available with hop/job guardrails.
- Stage/global dedup rules are available via `WithDedupRules(...)`.
- Stage rate limiting is available via `WithStageRateLimits(...)`.
- Cycle dedup key in `WithCycleMode(...)` is on a deprecation path in favor of `WithDedupRules(...)`.

## Milestones

## `v0.1.0` (released)

- [x] Stage workers runtime override (`WithStageWorkers`).
- [x] Stage workers docs and test coverage.
- [x] Hook and benchmark improvements bundled in first release.

## `v0.2.0` (released)

- [x] Stage rate limit controls (`WithStageRateLimits`).
- [x] Rate limit docs and test coverage.

## `v0.3.0` (released)

- [x] Dedup rules API (`WithDedupRules`).
- [x] Scope validation and dedup-drop hook behavior.
- [x] Cycle dedup compatibility routed through unified dedup runtime.
- [x] Deprecation docs for cycle dedup key.

## `v0.3.x` stabilization (next patch line)

- [ ] CI race-test gate (`go test -race ./...`).
- [ ] Add a focused test for `WithCycleMode + WithDedupRules` interaction.

## `v1.0.0` breaking cleanup

- [ ] Remove cycle dedup surface (`CycleModeOptions.DedupKey`).
- [ ] Change `WithCycleMode(maxHops, maxJobs, dedupKey)` to `WithCycleMode(maxHops, maxJobs)`.
- [ ] Remove cycle dedup compatibility shim from runtime.
- [ ] Remove `CycleDedupDrop` hook and standardize dedup drop reporting.
- [ ] Publish migration notes with before/after snippets.

## Feature Tracks

1. Runtime Observability
- Expand hooks/metrics coverage where needed.
- Keep no-hooks overhead minimal.

2. Retry and Timeout Controls
- Add stage-level retry policy and per-item timeout controls.
- Keep behavior opt-in and backward-compatible.

3. Frontier and Resume
- Define in-memory frontier abstraction for resume/retry semantics.
- Evaluate persistent frontier backends after API stabilizes.

4. Scanner/Crawler Patterns
- Document per-host limiter patterns inside stage processing.
- Add examples that combine global stage workers + per-host limits.

## Active TODO

- [x] Link the v1 breaking-change issue number in this doc (issue: `#1`).
- [x] Add `docs/migration-v1.md` draft before v1 API freeze.
- [ ] Keep release notes synchronized with completed milestone checkboxes.

## Working Agreement

Before each release:

- `gofmt -w .`
- `go vet ./...`
- `go test ./...`
- `go test -race ./...`
