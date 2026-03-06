# Frontier Guide

This guide explains frontier mode from a user perspective: when to use it, how to enable it, and what behavior to expect.

## What Frontier Mode Is

Frontier mode changes how work is scheduled internally:

- accepted items are queued in a frontier store
- a scheduler drains frontier entries and dispatches stage jobs
- job completion is tracked with acknowledge/retry semantics

For users, this mainly means a more explicit runtime path for enqueueing and dispatch under dynamic or high-volume workloads.

## Enable It

Use `WithFrontier(true)` when running a pipeline. For security workflows, this is the recommended runtime mode.

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"seed": {1, 2, 3}},
	pipex.WithFrontier[int](true),
)
```

Current code default is disabled (`WithFrontier(false)`) for backward compatibility.

Optional capacity override:

```go
pipex.WithFrontierPendingCapacity[int](4096)
```

If not set, capacity is auto-sized from runtime (`max(BufferSize * stageCount, 1024)`).

Optional blocking enqueue mode:

```go
pipex.WithFrontierBlockingEnqueue[int](true)
```

Default is `false` (non-blocking enqueue with backpressure retry loop).

## Behavior Guarantees

Frontier mode preserves core runtime semantics already expected from `Run`:

- dedup rules are still applied before work is accepted
- cycle-mode `MaxHops` and `MaxJobs` limits are still enforced
- cancellation is respected through run context
- seeds, triggers, and downstream fanout all use the same guarded enqueue path

Parity tests validate these behaviors in both frontier-off and frontier-on modes.

Frontier retry semantics are intentionally bounded by stage policy:

- stage attempt retries are controlled by `WithStagePolicies(...)`
- when a stage exhausts `MaxAttempts`, frontier does not requeue indefinitely
- exhausted stage errors are terminal for that frontier entry

## Backpressure Behavior

Under load, frontier mode applies enqueue backpressure instead of dropping accepted work immediately.

Current behavior:

- pending frontier capacity is user-configurable with `WithFrontierPendingCapacity(...)`
- if not configured, capacity is auto-sized (`max(BufferSize * stageCount, 1024)`)
- by default, if pending is temporarily full, enqueue retries with context-aware waiting
- with `WithFrontierBlockingEnqueue(true)`, enqueue blocks until space is available or context is canceled
- cancellation/timeout still aborts enqueue via `ctx.Done()`

This is intended to avoid queue-full failures during bursty workloads.

## Performance Expectations

Frontier mode has overhead compared to the direct path (`WithFrontier(false)`), because dispatch goes through frontier enqueue/reserve/ack.

Recent benchmark snapshot (machine-local, `BenchmarkRunFrontierComprehensive`):

- linear-success-256: frontier-on `+43%` vs frontier-off
- linear-success-2048: frontier-on `+74%` vs frontier-off
- fanout-success-512: frontier-on `+113%` vs frontier-off
- retry-exhaustion-mixed-512: frontier-on `+15%` vs frontier-off
- sink-slow-256: frontier-on `+7%` vs frontier-off

Interpretation:

- overhead is most visible in CPU-bound, high-fanout workloads
- overhead is less visible when downstream I/O dominates
- blocking enqueue mode can be significantly slower and should be enabled only when you need strict enqueue blocking semantics

## When To Use It

Use frontier mode when you value recoverable queued scheduling, explicit progress tracking, and controlled backpressure behavior.

Keep it off when raw throughput on simple CPU-bound flows is the top priority.

## Related Options

Frontier mode works alongside normal runtime options:

- `WithBufferSize(...)`
- `WithDedupRules(...)`
- `WithCycleMode(maxHops, maxJobs)`
- `WithFailFast(...)`
- `WithPartialResults(...)`

## Current Scope

What frontier mode currently provides:

- in-memory frontier-backed scheduling
- guarded enqueue parity with existing runtime semantics
- terminal handling for stage exhaustion (bounded by stage `MaxAttempts`)

What is not provided yet:

- persistent resume across process restarts
- public frontier backend API for external stores

## Failure Policy

Frontier mode uses strict consistency for frontier bookkeeping failures:

- `Ack` failure is treated as a run error.
- `Retry` failure is treated as a run error.

This avoids silently continuing with inconsistent frontier state.

## Frontier Hooks

Use hooks for frontier observability:

- `FrontierEnqueue`
- `FrontierReserve`
- `FrontierAck`
- `FrontierRetry`
