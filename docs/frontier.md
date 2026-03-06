# Frontier Guide

This guide explains frontier mode from a user perspective: when to use it, how to enable it, and what behavior to expect.

## What Frontier Mode Is

Frontier mode changes how work is scheduled internally:

- accepted items are queued in a frontier store
- a scheduler drains frontier entries and dispatches stage jobs
- job completion is tracked with acknowledge/retry semantics

For users, this mainly means a more explicit runtime path for enqueueing and dispatch under dynamic or high-volume workloads.

## Enable It

Use `WithFrontier(true)` when running a pipeline.

```go
res, err := p.Run(
	context.Background(),
	map[string][]int{"seed": {1, 2, 3}},
	pipex.WithFrontier[int](true),
)
```

Default is disabled (`WithFrontier(false)`).

Optional capacity override:

```go
pipex.WithFrontierPendingCapacity[int](4096)
```

If not set, capacity is auto-sized from runtime (`max(BufferSize * stageCount, 1024)`).

## Behavior Guarantees

Frontier mode preserves core runtime semantics already expected from `Run`:

- dedup rules are still applied before work is accepted
- cycle-mode `MaxHops` and `MaxJobs` limits are still enforced
- cancellation is respected through run context
- seeds, triggers, and downstream fanout all use the same guarded enqueue path

Parity tests currently validate these behaviors in both frontier-off and frontier-on modes.

## Backpressure Behavior

Under load, frontier mode can apply enqueue backpressure instead of dropping accepted work immediately.

Current behavior:

- pending frontier capacity is user-configurable with `WithFrontierPendingCapacity(...)`
- if not configured, capacity is auto-sized (`max(BufferSize * stageCount, 1024)`)
- if pending is temporarily full, enqueue retries with context-aware waiting
- cancellation/timeout still aborts enqueue via `ctx.Done()`

This is intended to avoid queue-full failures during bursty workloads.

## Performance Expectations

Frontier mode has overhead compared to the direct path.

Local benchmark snapshot (2-stage CPU-bound pipeline, 256 seeds, `WithBufferSize(256)`):

- frontier off: ~1.90 ms/op
- frontier on: ~2.65 ms/op
- overhead: ~1.39x (`+39%`)

Interpretation:

- this overhead may be less visible in I/O-heavy workloads
- for CPU-bound micro-pipelines, expect measurable scheduling/coordination cost

## When To Use It

Use frontier mode when you value explicit queued scheduling and controlled backpressure behavior.

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
