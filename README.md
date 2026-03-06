# pipex
A library to help you create pipelines in Golang

## Installation

Requires Go `1.25+`.

```bash
go get github.com/ruohao1/pipex@latest
```

## User Docs

- Hooks: `docs/hooks.md`
- Triggers: `docs/triggers.md`
- Sinks: `docs/sinks.md`
- Cycle mode: `docs/cycle-mode.md`
- Stage workers: `docs/stage-workers.md`
- Rate limits: `docs/rate-limits.md`
- Dedup rules: `docs/dedup.md`
- Product plan: `docs/product-plan.md`

## Run Options

`Run` supports optional runtime behavior via `Option`:

- `WithBufferSize(n)`: per-stage channel buffer size (`n > 0`).
- `WithFailFast(v)`: when `true`, cancel execution on first stage error.
- `WithFrontier(v)`: toggle frontier-backed enqueue/ack bookkeeping.
- `WithTriggers(...)`: register trigger sources that emit items during runtime.
- `WithSinks(...)`: register sinks that consume stage outputs per item during runtime.
- `WithHooks(...)`: register runtime observability callbacks.
- `WithSinkRetry(maxRetries, backoff)`: configure sink retry policy.
- `WithPartialResults(v)`: when `true`, return currently collected results together with an error.
- `WithStageWorkers(map[string]int)`: per-run per-stage worker overrides (must reference existing stages; each value must be `> 0`).
- `WithStageRateLimits(map[string]RateLimit)`: per-run per-stage rate limits (`RPS > 0`, `Burst >= 1`).
- `WithStagePolicies(map[string]StagePolicy)`: per-run per-stage retry/timeout policy (`MaxAttempts >= 1`, `Backoff >= 0`, `Timeout >= 0`).
- `WithDedupRules(...)`: per-run dedup rules (`global` or `stage:<name>` scope).
- `WithCycleMode(maxHops, maxJobs)`: cycle traversal guardrails (hop and max-jobs limits). Use `WithDedupRules(...)` for dedup behavior.

Defaults:

- `BufferSize`: `1024`
- `FailFast`: `false`
- `UseFrontier`: `false`
- `SinkRetry.MaxRetries`: `10`
- `SinkRetry.Backoff`: `10ms`

## Error Behavior

- Pipeline config/graph validation errors are returned before execution starts.
- If stage processing errors occur, `Run` returns a joined error (`errors.Join(...)`).
- Trigger and sink errors are also joined into the returned error.
- Invalid `WithStageWorkers(...)` configuration (unknown stage or non-positive worker count) returns a run error before processing starts.
- Invalid `WithStageRateLimits(...)` configuration (unknown stage, non-positive `RPS`, or `Burst < 1`) returns a run error before processing starts.
- Invalid `WithStagePolicies(...)` configuration (unknown stage, `MaxAttempts < 1`, negative backoff, or negative timeout) returns a run error before processing starts.
- Invalid `WithDedupRules(...)` configuration (empty name/scope, nil key, invalid scope format, or unknown stage in `stage:<name>`) returns a run error before processing starts.
- When `FailFast=true`, the first stage error cancels the run, and the stage error is returned (not `context.Canceled`).
- If there are no stage errors but the context is canceled or times out, `Run` returns the context error.
- By default, errors return `nil` results.
- With `WithPartialResults(true)`, `Run` returns partial results plus the error.

## Trigger Lifecycle

- Triggers emit items through `emit(...)` into their configured stage, just like dynamic seeds.
- `Run` waits for all triggers to return from `Start(...)` before it can fully drain and close the pipeline.
- A non-terminating trigger keeps `Run` alive until context cancellation.

## Sink Lifecycle

- Sinks consume items from one configured stage (`Sink.Stage()`).
- Delivery is per output item (`Consume(ctx, item)`), not batch-based.
- On sink failure, the same item is retried with `WithSinkRetry(maxRetries, backoff)`.
- If retries are exhausted:
  - the sink error is recorded,
  - that sink is disabled for the remainder of the run,
  - pipeline processing continues unless `WithFailFast(true)` is enabled.
- On cancellation, sink workers stop via `ctx.Done()`.

## Hooks

- Hooks provide runtime observability and do not alter pipeline control flow.
- Run-level callbacks: `RunStart`, `RunEnd`.
- Stage callbacks: `StageStart`, `StageFinish`, `StageError`.
- Trigger callbacks: `TriggerStart`, `TriggerEnd`, `TriggerError`.
- Sink callbacks: `SinkConsumeStart`, `SinkConsumeSuccess`, `SinkRetry`, `SinkExhausted`.

Minimal example:

```go
hooks := pipex.Hooks[int]{
	RunStart: func(ctx context.Context, meta pipex.RunMeta) {
		fmt.Println("run start", meta.RunID)
	},
	RunEnd: func(ctx context.Context, meta pipex.RunMeta, err error) {
		fmt.Println("run end", meta.RunID, "err:", err)
	},
}

_, _ = p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2, 3}},
	pipex.WithHooks[int](hooks),
)
```

See detailed contract: `docs/hooks.md`.

## Retry Policy Guide

- Use bounded retries for most sinks:
  - `WithSinkRetry(3..20, 10ms..250ms)` is a good starting range.
  - This avoids permanently stuck runs when a downstream dependency is unhealthy.
- Use `MaxRetries=-1` only for must-deliver workloads where the run is allowed to wait indefinitely.
- Keep backoff non-zero to avoid tight retry loops and unnecessary CPU churn.
- Use `WithFailFast(true)` when sink failures should abort the whole run quickly.

## Common Pitfalls

- Non-terminating triggers will keep `Run` alive. Always provide a cancellable context for long-lived triggers.
- Sinks with persistent failures will eventually disable themselves after retry exhaustion unless `MaxRetries=-1`.
- Using `MaxRetries=-1` means infinite retry and can keep a run alive if the sink never succeeds.
- If you need progress on failure/cancellation, enable `WithPartialResults(true)`.
- Treat stage definitions as immutable after `AddStage(...)`:
  - Keep `Stage.Name()` and `Stage.Workers()` stable for the lifetime of the stage registration.
  - Mutating `Workers()` between `AddStage(...)` and `Run(...)` can cause runtime pool creation failures.
  - If you need dynamic tuning, prefer explicit run-time options (for example a future worker-override option) over mutable stage internals.

## Scanner Concurrency Pattern

- Use stage workers (`Stage.Workers()` or `WithStageWorkers(...)`) as global per-stage concurrency limits.
- For per-host limits (for example HTTP scanning), enforce host-keyed throttling inside the network stage itself.
- Typical scanner setup combines both:
  - global cap at the stage level for total throughput,
  - per-host cap in-stage to avoid overloading single targets.

## Quickstart

```go
p := pipex.NewPipeline[int]()
_ = p.AddStage(srcStage)
_ = p.AddStage(outStage)
_ = p.Connect("src", "out")

res, err := p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2, 3}},
	pipex.WithBufferSize[int](64),
	pipex.WithFailFast[int](true),
)
```

Full runnable example:

```bash
go run ./examples/quickstart
```

## Sink Snippet

```go
type collectSink struct{ /* fields */ }
func (s *collectSink) Name() string  { return "collector" }
func (s *collectSink) Stage() string { return "out" }
func (s *collectSink) Consume(ctx context.Context, item int) error {
	// write to DB, queue, metrics, etc.
	return nil
}

sink := &collectSink{}
_, err := p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2, 3}},
	pipex.WithSinks[int](sink),
	pipex.WithSinkRetry[int](10, 10*time.Millisecond),
)
```

Full runnable sink example:

```bash
go run ./examples/sink
```

## Cycle Mode Example

```bash
go run ./examples/cycle
```

For cycle mode options and behavior details, see `docs/cycle-mode.md`.
