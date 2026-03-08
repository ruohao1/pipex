# pipex
A Go library for building staged execution pipelines.

## Positioning

`pipex` is an execution engine library, not the scanner product (`penta`):
https://github.com/ruohao1/penta

Engine-scanner ownership boundaries are defined in `docs/boundary.md`.

## Installation

Requires Go `1.25+`.

```bash
go get github.com/ruohao1/pipex@latest
```

## Quickstart (Primary Path)

Use this path first: DAG pipeline + frontier mode for recoverable scheduling.

```go
p := pipex.NewPipeline[int]()
_ = p.AddStage(srcStage)
_ = p.AddStage(outStage)
_ = p.Connect("src", "out")

res, err := p.Run(
	context.Background(),
	map[string][]int{"src": {1, 2, 3}},
	pipex.WithFrontier[int](true),
	pipex.WithBufferSize[int](64),
	pipex.WithFailFast[int](true),
)
_ = res
_ = err
```

Run the full example:

```bash
go run ./examples/quickstart
```

## Core Runtime Notes

- Pipeline config/graph validation errors are returned before execution starts.
- Stage/trigger/sink runtime errors are joined and returned from `Run`.
- In frontier mode, frontier `Ack`/`Retry` failures are treated as run errors.
- By default, errors return `nil` results.
- With `WithPartialResults(true)`, `Run` returns partial results plus the error.

## Advanced Features

- Triggers: `docs/triggers.md`
- Sinks and sink retry: `docs/sinks.md`
- Hooks and observability: `docs/hooks.md`
- Stage workers: `docs/stage-workers.md`
- Stage rate limits: `docs/rate-limits.md`
- Stage retry/timeout policies: documented in API and tests
- Dedup rules: `docs/dedup.md`
- Cycle mode: `docs/cycle-mode.md`
- Frontier details: `docs/frontier.md`

Additional docs:

- Product direction: `docs/product-plan.md`
- Architecture boundaries: `docs/architecture.md`
- Engine-scanner boundary: `docs/boundary.md`
- Session runtime semantics: `docs/session-runtime.md`

## Examples

- Quickstart: `go run ./examples/quickstart`
- Sink integration: `go run ./examples/sink`
- Cycle mode: `go run ./examples/cycle`
