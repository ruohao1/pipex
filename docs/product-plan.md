# Product Plan

This document tracks feature planning, milestones, and active TODOs for `pipex`.

## Planning Goals

- Position `pipex` as a recoverable workflow runtime for security automation.
- Keep an ephemeral direct-dispatch mode for low-overhead local execution and benchmarks.
- Evolve runtime safety, retry behavior, and observability without breaking API ergonomics.

## Current Status

- Per-stage worker pools and run-level worker overrides are available.
- Trigger and sink runtime integrations are available.
- Cycle mode is available with hop/job guardrails.
- Stage/global dedup rules are available via `WithDedupRules(...)`.
- Stage rate limiting is available via `WithStageRateLimits(...)`.
- Stage retry/timeout policies are available via `WithStagePolicies(...)`.
- Frontier-backed runtime mode is available via `WithFrontier(true)`.
- Ephemeral direct-dispatch mode is available via `WithFrontier(false)`.
- Cycle dedup surface has been removed in favor of `WithDedupRules(...)`.

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

## `v0.3.1` (released)

- [x] CI race-test gate (`go test -race ./...`).
- [x] Added focused interaction tests for cycle mode + dedup rules.
- [x] Added stage retry/timeout policies and attempt-level hook coverage.

## `v1.0.0` breaking cleanup

- [x] Remove cycle dedup surface (`CycleModeOptions.DedupKey`).
- [x] Change `WithCycleMode(maxHops, maxJobs, dedupKey)` to `WithCycleMode(maxHops, maxJobs)`.
- [x] Remove cycle dedup compatibility shim from runtime.
- [x] Remove `CycleDedupDrop` hook and standardize dedup drop reporting.
- [x] Publish migration notes with before/after snippets.

## `v1.1.x` frontier hardening (active)

- [x] Ensure stage exhaustion is terminal in frontier mode (no unbounded requeue).
- [x] Make memory frontier enqueue/close behavior deterministic.
- [x] Preserve inflight entries on retry backpressure (`ErrPendingQueueFull`).
- [x] Add parity and frontier-focused benchmark coverage.
- [ ] Define explicit frontier execution-state model (`pending`, `reserved`, `acked`, `retried`, `terminal_failed`, `dropped`, `canceled`).
- [ ] Add terminal failure recording/reporting path (beyond ack/retry only).
- [ ] Add frontier runtime metrics surface (queue depth, inflight, retries, per-stage backlog).
- [ ] Stabilize blocking enqueue mode under retry-heavy contention and re-enable it in all benchmark scenarios.

## Feature Tracks

1. Runtime Observability
- Expand hooks/metrics coverage where needed.
- Keep no-hooks overhead minimal.

2. Retry and Timeout Controls
- Add stage-level retry policy and per-item timeout controls.
- Keep behavior opt-in and backward-compatible.

3. Frontier Runtime (Primary)
- Keep frontier-backed execution as the canonical runtime path for security workflows.
- Preserve `WithFrontier(false)` as an ephemeral fast mode and performance baseline.
- Harden frontier semantics and observability before introducing persistent backends.
- Evaluate persistent frontier backends after execution-state API stabilizes.

4. Scanner/Crawler Patterns
- Document per-host limiter patterns inside stage processing.
- Add examples that combine global stage workers + per-host limits.

## Active TODO

- [x] Link the v1 breaking-change issue number in this doc (issue: `#1`).
- [x] Add `docs/migration-v1.md` draft before v1 API freeze.
- [x] Keep release notes synchronized with completed milestone checkboxes.
- [ ] Publish a runtime-mode guide comparing recoverable (frontier) and ephemeral modes.
- [ ] Add dedicated microbenchmarks isolating frontier store vs scheduler vs hooks costs.

## Working Agreement

Before each release:

- `gofmt -w .`
- `go vet ./...`
- `go test ./...`
- `go test -race ./...`
