# Session Runtime (Engine-Level)

## Purpose

Define resumable runtime semantics for `pipex` without introducing scanner-specific domain concepts.

This document describes execution session primitives only. Scanner domain semantics (`scope`, `target`, `finding`, UX) remain owned by `penta`.

## Boundary

- `pipex` owns execution identity, queue/runtime state, and resume behavior.
- `penta` owns scanner domain models and user-facing session workflow.

## Identity Model

- `run_id`: unique execution identifier (already present in runtime hooks/metadata).
- `runtime_namespace` (optional): logical partition key for durable frontier storage to isolate concurrent execution domains.
- `entry_key`: durable/idempotent unit key for frontier entries (store-level concern).

`pipex` should not define scanner IDs; mapping from scanner session IDs to runtime identity is application-owned.

## Execution States

Runtime state model for resumable execution:

- `pending`: accepted and queued for processing.
- `reserved`: leased to a worker/scheduler path.
- `retried`: requeued after failure.
- `acked`: completed terminal success.
- `terminal_failed`: completed terminal failure (non-retriable/exhausted).
- `dropped`: intentionally skipped by runtime rule (for example dedup/hop guard).
- `canceled`: canceled by context/session stop.

## Lifecycle Semantics

1. Start
- New run starts with `run_id`.
- Seeds/triggers enqueue to frontier/direct runtime as configured.

2. Pause (engine semantic)
- Stop accepting new dispatch from scheduler.
- Keep durable runtime state intact (`pending`/`reserved` entries preserved for resume).
- Inflight items are allowed to settle to terminal/ retryable state according to policy.

3. Resume
- Rehydrate scheduler from durable frontier state.
- Requeue expired/abandoned `reserved` leases according to lease policy.
- Continue dispatch from `pending` and requeued entries.

4. Cancel
- Stop scheduling and mark run canceled from runtime perspective.
- Outstanding entries transition per policy (`canceled` or retained for later resume if cancellation policy allows).

## Failure and Idempotency Rules

- Requeue of expired reservations must be idempotent.
- Ack/retry operations must be monotonic by entry state (invalid transitions are explicit errors).
- Resume should be safe to call repeatedly (no duplicate terminal transitions).
- Entry key uniqueness should prevent duplicate durable entry creation where supported by store.

## Minimal Internal API Proposal (No Public Break Yet)

Initial internal-facing shape (illustrative):

- `runtime.StartRun(ctx, cfg) -> runRef`
- `runtime.PauseRun(ctx, runRef) error`
- `runtime.ResumeRun(ctx, runRef) error`
- `runtime.CancelRun(ctx, runRef) error`
- `runtime.RunStatus(ctx, runRef) -> status`

Where:

- `runRef` contains engine identity (`run_id`, optional namespace/store refs).
- `status` reports aggregate runtime counters by state, not scanner domain objects.

These can remain internal until semantics stabilize.

## Out of Scope (For This Doc)

- Scanner-specific workflow state machine.
- TUI interaction flows.
- Findings/result storage schema.
- Public API commitment for pause/resume until internal behavior hardens.

## Next Implementation Slice

- Define durable lease-expiry requeue policy constants and tests.
- Add internal run status snapshot aggregation for durable frontier stores.
- Validate resume idempotency under repeated calls and partial failures.
