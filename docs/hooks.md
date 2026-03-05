# Hooks

Hooks let you observe `Pipeline.Run` lifecycle events.

- Hooks are optional.
- Hooks are best-effort and must not change pipeline control flow.
- Hook callbacks may run concurrently; implementations should be thread-safe.

## Enable Hooks

Use `WithHooks(...)` in `Run`:

```go
hooks := pipex.Hooks[int]{
	RunStart: func(ctx context.Context, meta pipex.RunMeta) {
		fmt.Println("run start:", meta.RunID)
	},
	RunEnd: func(ctx context.Context, meta pipex.RunMeta, err error) {
		fmt.Println("run end:", meta.RunID, "err:", err)
	},
}

_, err := p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2, 3}},
	pipex.WithHooks[int](hooks),
)
```

## Run Metadata

`RunStart` and `RunEnd` receive `RunMeta`:

- `RunID`
- `SeedStages`
- `SeedItems`
- `StageCount`
- `EdgeCount`
- `TriggerCount`
- `SinkCount`
- `FailFast`
- `BufferSize`

## Available Callbacks

Run-level:

- `RunStart(ctx, meta)`
- `RunEnd(ctx, meta, err)`

Stage-level:

- `StageStart(ctx, StageStartEvent[T])`
- `StageFinish(ctx, StageFinishEvent[T])`
- `StageError(ctx, StageErrorEvent[T])`

Trigger-level:

- `TriggerStart(ctx, TriggerStartEvent[T])`
- `TriggerEnd(ctx, TriggerEndEvent[T])`
- `TriggerError(ctx, TriggerErrorEvent[T])`

Sink-level:

- `SinkConsumeStart(ctx, SinkConsumeStartEvent[T])`
- `SinkConsumeSuccess(ctx, SinkConsumeSuccessEvent[T])`
- `SinkRetry(ctx, SinkRetryEvent[T])`
- `SinkExhausted(ctx, SinkExhaustedEvent[T])`

## Event Ordering

Ordering is guaranteed only for local lifecycle paths:

- Stage item: `StageStart` -> (`StageFinish` or `StageError`)
- Trigger run: `TriggerStart` -> (`TriggerEnd` or `TriggerError`)
- Sink attempt: `SinkConsumeStart` -> (`SinkConsumeSuccess` or `SinkRetry` or `SinkExhausted`)

Global ordering across workers/stages is not guaranteed.

## Sink Retry Notes

- `SinkRetry` is emitted after a failed consume attempt and before backoff sleep.
- `SinkExhausted` is emitted once when retry budget is exceeded for that sink item path.

## Safety Notes

- Hook panics are recovered; pipeline execution continues.
- Keep callbacks lightweight and non-blocking to avoid throughput impact.
