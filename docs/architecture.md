# Pipex Architecture Boundaries and Runtime Map

## Purpose

This document defines:

1. Ownership boundaries for `internal/graph` and `internal/runtime`.
2. Current runtime decomposition (`Pipeline.Run` facade -> runtime helpers).
3. Rules for future extractions without API behavior changes.

`Pipeline.Run` remains the public orchestration entrypoint. Internal execution details are progressively delegated to `internal/runtime`.

## 1) Ownership Boundaries

### `internal/graph`

Scope:

- Graph validation rules (stage existence checks, edge validity).
- DAG cycle detection and related graph utilities.
- Pure graph traversal helpers.

Non-scope:

- Worker pools, goroutine lifecycle, context cancellation handling.
- Trigger/sink execution logic.
- Runtime scheduling and queue coordination.

Design rule:

- Keep `internal/graph` deterministic and side-effect light.

### `internal/runtime`

Scope:

- Pipeline run orchestration helpers and lifecycle coordination.
- Frontier scheduler, enqueue, and gate logic.
- Stage execution policy loops (attempt/retry/timeout behavior).
- Concurrency helpers around queueing/scheduling.

Non-scope:

- Graph-structural validation/cycle algorithms.
- Public API contract definitions (`Stage`, `Sink`, `Trigger`, option signatures).

Design rule:

- `internal/runtime` owns reusable execution mechanics.
- Root package (`pipex`) remains the policy facade that maps runtime hooks/errors to public contracts.

## 2) Current Runtime Map

`Pipeline.Run` currently delegates these responsibilities:

- `validateRunPreflight(...)`: run-time input/option preflight checks.
- `buildDedupRulesByStage(...)`: dedup rules validation and expansion.
- `buildStageLimiters(...)`: per-stage rate limiter setup.
- `createPoolsByStage(...)`: pool creation and worker snapshot.
- `startSinkWorkers(...)`: sink worker lifecycle.
- `startStagePools(...)`: stage pool startup and error fan-in.
- `enqueueSeeds(...)`: initial seed enqueue path.
- `startTriggerWorkers(...)`: trigger lifecycle and emit path.
- `waitForFrontierOutstanding(...)`: frontier drain/wait shutdown path.
- `setupFrontierRuntime(...)`: frontier store/stats/requeue setup.

`internal/runtime` currently provides:

- `RunFrontierScheduler(...)` (`frontier_scheduler.go`): reserve/dispatch loop.
- `EnqueueWithBackpressure(...)` (`frontier_enqueue.go`): non-blocking frontier backpressure retry.
- `SafeStringKey(...)` (`dedup.go`): panic-safe dedup key evaluation.
- `EvaluateCycleGate(...)` and `SeenContainsOrInsert(...)` (`gates.go`): cycle/dedup gate utilities.
- `ExecuteGuardedEnqueue(...)` (`guarded_enqueue.go`): guarded enqueue orchestration.
- `EnqueueDirect(...)` (`direct_enqueue.go`): direct queue enqueue mechanics.
- `ExecuteStageWithPolicy(...)` (`stage_execute.go`): attempt/retry/timeout execution loop.

## 3) Extraction Rules

Move logic from `pipeline.go` to `internal/runtime` only when all conditions hold:

1. Cohesion is clear.
- The extracted code serves one execution concern and has a narrow config surface.

2. Public API stability is preserved.
- No exported signatures change.
- `Pipeline.Run` remains the single public runtime entrypoint.

3. Behavioral parity is proven.
- Existing integration tests stay green.
- Add focused `internal/runtime` tests for each extracted helper.

4. Dependency direction stays clean.
- Root package may depend on `internal/runtime`.
- `internal/runtime` must not import root package types.

5. Error/hook mapping remains in root package when domain-specific.
- Runtime helpers should expose callback hooks/results.
- Root package converts callbacks/results into public events/errors.

## 4) Next Targets

- Continue reducing closure-heavy wiring in `Pipeline.Run` by grouping helper configs into smaller typed bundles.
- Keep extraction slices small and independently reviewable.
- Maintain parity checks with:
  - `go test ./...`
  - `go test -race ./...`
