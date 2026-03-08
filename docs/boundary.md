# Engine-Scanner Boundary

## Purpose

This document defines a hard boundary between:

- `pipex` (generic execution engine library)
- `penta` (security scanner application): https://github.com/ruohao1/penta

`pipex` must remain domain-neutral even when primarily used by security tooling.

## Engine Owns (`pipex`)

- Pipeline graph composition and validation.
- Runtime orchestration (workers, scheduling, cancellation, retries/timeouts, rate limits).
- Frontier/store abstractions for recoverable execution mechanics.
- Generic runtime events/hooks and error contracts.
- Neutral runtime identifiers (`RunID`, stage names, entry IDs, attempts, timestamps).

## Penta Owns (Application Layer)

- Security domain model (`Session`, `Scope`, `Target`, `Asset`, `Finding`, `Severity`, etc.).
- Scan policy and strategy (what to scan, enqueue/resume/stop rules).
- TUI/UX behavior and operator workflow.
- Domain persistence, reporting, and result interpretation.

## Adapter Contract (Penta -> Pipex)

Penta should use a thin adapter layer (for example `internal/enginebridge`) to isolate `pipex`:

- `StartSession(ctx, sessionID, cfg, seeds) (RunRef, error)`
- `ResumeSession(ctx, sessionID) (RunRef, error)`
- `PauseSession(ctx, sessionID) error`
- `CancelSession(ctx, sessionID) error`
- `SessionStatus(ctx, sessionID) (Status, error)`

`RunRef` contains engine-facing identifiers and metadata only (for example `RunID`, store namespace/keyspace).

## Data Ownership

- Engine store/state:
  - execution queue state (`pending`, `reserved`, `acked`, `retried`, terminal states),
  - execution metadata required for runtime recovery.
- Penta datastore:
  - session metadata,
  - scope/target inventories,
  - findings/results,
  - operator/audit metadata.
- Mapping is penta-owned:
  - `session_id -> run_id`
  - optional `session_id -> engine namespace`

## API Admission Rule

A new API belongs in `pipex` only if it is broadly useful beyond scanner workflows.

If a proposal depends on scanner-specific language or semantics, it must remain in `penta`.

## Non-Goals for `pipex`

- No scanner-specific public types.
- No vulnerability semantics.
- No scan profile/business policy API.
- No TUI/session UX API.
