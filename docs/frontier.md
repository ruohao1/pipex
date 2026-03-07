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

Latest benchmark snapshot (machine-local, March 7, 2026):

Command:

```bash
go test -run '^$' -bench BenchmarkRunFrontierMode/frontier-on -benchmem -count=5 ./...
```

Environment:

- `goos=linux`
- `goarch=amd64`
- CPU: `13th Gen Intel(R) Core(TM) i7-1360P`

Median results:

- `frontier-on`: `2,014,925 ns/op`, `291,112 B/op`, `1,170 allocs/op`
- `frontier-on-sampled-stats`: `2,402,678 ns/op` (`+19.2%` vs frontier-on), `289,774 B/op`, `1,180 allocs/op`
- `frontier-on-per-item-hooks`: `2,202,945 ns/op` (`+9.3%` vs frontier-on), `290,081 B/op`, `1,170 allocs/op`

Compared with the earlier local `frontier-on` median (`2,665,843 ns/op`, `319,597 B/op`, `2,205 allocs/op`), current runtime is approximately:

- `~24.4%` faster (`ns/op`)
- `~8.9%` lower memory (`B/op`)
- `~46.9%` fewer allocations (`allocs/op`)

Interpretation:

- frontier bookkeeping still dominates overhead versus direct dispatch
- sampled frontier stats are materially cheaper than baseline frontier cost, but are not free
- per-item hooks add overhead and should be enabled selectively in production

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
- `WithFrontierStatsInterval(...)`

## Recommended Defaults

For production-style security workflows, start with:

- `WithFrontier(true)`
- `WithBufferSize(256)` (or higher for bursty fanout)
- leave `WithFrontierPendingCapacity(...)` unset first (auto sizing), then tune with `FrontierStats` data
- `WithFrontierStatsInterval(250 * time.Millisecond)` for low-cost runtime visibility
- keep per-item frontier hooks disabled by default unless needed for debugging

Example:

```go
res, err := p.Run(
	context.Background(),
	seeds,
	pipex.WithFrontier[int](true),
	pipex.WithBufferSize[int](256),
	pipex.WithFrontierStatsInterval[int](250*time.Millisecond),
)
```

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
- `FrontierStats` (sampled snapshot; enabled via `WithFrontierStatsInterval(...)`)
