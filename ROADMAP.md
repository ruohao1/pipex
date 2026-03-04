# Pipex Roadmap

## Goal
Build a trigger-driven Go pipeline library where stages can execute sequentially or in parallel, and downstream stages can start as soon as upstream emits values.

## Target Repository Structure (v0)

```text
pipex/
  go.mod
  README.md

  pipeline.go          // public: Pipeline, Builder, Connect, Run
  stage.go             // public: Stage, StageFunc, StageConfig
  trigger.go           // public: Trigger, TriggerSource
  compose.go           // public: Sequence, Parallel, FanOut, Merge
  options.go           // public: RunOptions, WithBuffer, WithFailFast
  errors.go            // public errors

  job.go               // public: Job, JobFunc
  pool.go              // public: Pool, PoolConfig

  internal/
    graph/
      dag.go           // validate, cycle detection, topo helpers
      node.go          // internal node/edge models
    runtime/
      scheduler.go     // orchestrates stage workers + triggers
      worker.go        // per-stage worker loop
      channels.go      // channel wiring and fanout helpers
      cancellation.go  // context/error propagation

  examples/
    basic/main.go
    fanout/main.go
    cancellation/main.go
```

## Public API Targets

Core types:
- `Pipeline[T any]`
- `Builder[T any]`
- `Stage[T any]`
- `StageFunc[T any]`
- `StageConfig`
- `Trigger[T any]`
- `RunOptions`
- `Job`, `JobFunc`
- `Pool`, `PoolConfig`

Core constructors/functions:
- `New[T any]() *Builder[T]`
- `(b *Builder[T]) AddStage(cfg StageConfig, fn StageFunc[T]) *Builder[T]`
- `(b *Builder[T]) Connect(from, to string) *Builder[T]`
- `(b *Builder[T]) Build() (*Pipeline[T], error)`
- `(p *Pipeline[T]) Run(ctx context.Context, in map[string][]T, opts ...Option) (map[string][]T, error)`
- `Sequence`, `Parallel`, `FanOut`, `Merge`

## Implementation Phases

1. Foundation
- Introduce `Builder`, `StageConfig`, `RunOptions`, and error types.
- Move DAG validation to `internal/graph`.

2. Runtime
- Move execution logic to `internal/runtime/scheduler.go`.
- Add explicit cancellation and fail-fast behavior.

3. Composition
- Implement `Sequence`, `Parallel`, `FanOut`, `Merge` in `compose.go`.

4. Quality
- Add race-focused tests (`go test -race ./...`).
- Add integration tests for fanout, cancellation, and error propagation.

5. Usability
- Add examples under `examples/`.
- Update `README.md` with a quickstart and architecture notes.

## Development Checklist
- Format: `gofmt -w .`
- Vet: `go vet ./...`
- Unit tests: `go test ./...`
- Race checks: `go test -race ./...`
