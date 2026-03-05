# Pipex Architecture Boundaries and Extraction Plan

## Purpose

This document defines:

1. Ownership boundaries for `internal/graph` and `internal/runtime`.
2. Criteria for when code should move from root package files into `internal/`.
3. The first migration slice to start using `internal/` without changing public API.

The current implementation remains rooted in `pipeline.go` and `compose.go`. The goal is to extract cohesive internals incrementally while preserving behavior.

## 1) Ownership Boundaries

### `internal/graph`

Scope:

- Graph validation rules (stage existence checks, edge validity).
- DAG cycle detection and future cycle-mode helpers.
- Traversal helpers that are pure graph concerns (topological/frontier utilities).
- Future dedup/frontier graph metadata (if graph-structural, not runtime-stateful).

Non-scope:

- Worker pools, goroutine lifecycle, context cancellation handling.
- Trigger/sink execution logic.
- Item processing orchestration.

Design rule:

- Keep `internal/graph` deterministic and side-effect light.
- Prefer pure functions and immutable snapshots as inputs.

### `internal/runtime`

Scope:

- Pipeline run orchestration and lifecycle coordination.
- Enqueue/drain mechanics, stage worker orchestration, waitgroup ordering.
- Trigger and sink coordination across run lifecycle.
- Error aggregation and cancellation policy application.
- Future runtime policies (stage retry/timeout, rate limit enforcement hooks).

Non-scope:

- Graph-structural validation/cycle algorithms.
- Public API contract definitions (`Stage`, `Sink`, `Trigger`, option signatures).

Design rule:

- `internal/runtime` owns concurrency semantics; external packages should not reimplement them.
- Runtime should execute against stable stage metadata captured at run start (for example, worker count), not mutable values that may drift mid-lifecycle.

## 2) Extraction Criteria

Move code from root into `internal/` only when all conditions hold:

1. Cohesion is clear.
- The code cluster serves one concern (`graph` or `runtime`) without requiring broad cross-file context.

2. Public API stability is preserved.
- No exported type/function signatures need to change.
- Root package remains the stable facade (`Pipeline.Run`, `Validate`, composition helpers).

3. Behavioral parity is provable.
- Existing tests still express the same semantics.
- No expected ordering/cancellation/error behavior changes in the migration commit.

4. Complexity threshold is reached.
- A function/file mixes multiple concerns (for example, validation + orchestration), making maintenance harder than delegation.

5. Dependency direction remains clean.
- Root package may delegate to `internal/*`.
- `internal/graph` must not depend on `internal/runtime`.
- `internal/runtime` can depend on `internal/graph` only via narrow inputs if needed.

6. Migration unit is independently reviewable.
- A single extraction change should be understandable without requiring future planned refactors.

## 3) First Migration Slice

### Target

Extract graph validation logic from `pipeline.go` into `internal/graph` first.

Recommended extraction scope:

- Stage/edge existence checks currently in validation path.
- DAG cycle detection currently in `validateSnapshot`.

Keep in root package:

- Public method entry points (`Validate`, `Run` preflight checks).
- Error construction and public error types in `errors.go`.

### Why this first

- Lowest risk: graph validation is relatively pure and easier to isolate.
- High clarity gain: reduces mixed responsibilities in `pipeline.go`.
- No runtime-concurrency behavior impact.

### Migration shape

1. Introduce internal graph validator API that accepts stage/edge snapshots.
2. Delegate `validateSnapshot` internals to `internal/graph`.
3. Keep outward error behavior unchanged (`ErrNoStages`, `ErrCycle`, `StageNotFound` wrapping semantics).
4. Keep all existing tests green with no behavioral edits.

### Deferred until later

Do not extract runtime orchestration yet (`Run` goroutine choreography, sink/trigger lifecycle), because hooks/retry policy design may still evolve and would cause churn.
