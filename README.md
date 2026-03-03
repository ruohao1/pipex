# pipex
A library to help you create pipelines in Golang

## Run Options

`Run` supports optional runtime behavior via `Option`:

- `WithBufferSize(n)`: per-stage channel buffer size (`n > 0`).
- `WithFailFast(v)`: when `true`, cancel execution on first stage error.

Defaults:

- `BufferSize`: `1024`
- `FailFast`: `false`

## Error Behavior

- Pipeline config/graph validation errors are returned before execution starts.
- If stage processing errors occur, `Run` returns a joined error (`errors.Join(...)`).
- When `FailFast=true`, the first stage error cancels the run, and the stage error is returned (not `context.Canceled`).
- If there are no stage errors but the context is canceled or times out, `Run` returns the context error.
