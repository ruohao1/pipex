# Repository Guidelines

## Project Structure & Module Organization
This repository is a Go library for building reusable pipelines: `github.com/ruohao1/pipex`.
Root files include `go.mod` (module path and Go version) and `README.md` (high-level description).

Production code currently lives at the module root (`pipeline.go`, `compose.go`, `pool.go`, etc.). Keep tests next to source files using `_test.go`.

`examples/` contains runnable programs:
- `examples/quickstart`
- `examples/sink`

`internal/` is currently minimal and reserved for non-public runtime pieces once they are stabilized (for example, frontier scheduler/dedup utilities).

## Build, Test, and Development Commands
Use standard Go tooling from the repository root:

- `go test ./...`: run all unit tests across packages
- `go test -cover ./...`: run tests with coverage output
- `go build ./...`: verify all packages compile
- `go vet ./...`: run static checks for suspicious constructs
- `gofmt -w .`: format all Go files in-place

Run formatting and tests before opening a pull request.

## Coding Style & Naming Conventions
- Follow idiomatic Go style and `gofmt` output (tabs for indentation).
- Use short, descriptive package names (`pipeline`, not `pipeline_utils`).
- Exported identifiers use `PascalCase`; unexported identifiers use `camelCase`.
- Keep interfaces small and behavior-focused (for example, `Runner`, `Stage`).
- Prefer table-driven tests for core pipeline behaviors.

## Testing Guidelines
- Use Go’s built-in `testing` package.
- Name test files `*_test.go` and test functions `TestXxx`.
- Use subtests (`t.Run(...)`) for scenario coverage where helpful.
- Aim to cover success paths, error propagation, and edge-case stage ordering.

## Commit & Pull Request Guidelines
Current history is minimal (`Initial commit`), so follow a simple, consistent convention:

- Commit messages: imperative, concise subject line (e.g., `Add pipeline stage validation`).
- Keep commits focused on one logical change.
- PRs should include what changed and why, test evidence (`go test ./...` summary), a linked issue when applicable, and an API usage example when signatures or behavior change.

## Agent Collaboration
- Default mode for AI assistance in this repository is review and guidance only.
- Do not edit code, create files, or run write operations unless the user explicitly asks for implementation.
- Prefer design feedback, API review, bug/risk findings, and test gap analysis when no coding request is given.
- The assistant should write or update tests when asked, and may proactively add missing tests to validate behavior under review.

## Current Product Notes
- Pipeline execution uses per-stage worker pools.
- Sinks consume per-item outputs (`Consume(ctx, item)`), with retry controls via `WithSinkRetry`.
- Graph validation is DAG-only by default (no cycles).
