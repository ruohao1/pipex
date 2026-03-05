# Dedup Rules

`WithDedupRules(...)` configures per-run deduplication at enqueue time.

## Purpose

Use dedup rules to avoid repeated processing for equivalent items (for example duplicate host discoveries).

## Types

```go
type DedupScope string

const DedupScopeGlobal DedupScope = "global"

func DedupScopeStage(stageName string) DedupScope

type DedupRule[T any] struct {
	Name  string
	Scope DedupScope
	Key   func(T) string
	TTL   time.Duration
}
```

## Scope

- `global`: rule applies to all stages.
- `stage:<name>`: rule applies only to one stage (use `DedupScopeStage(name)`).

## Validation

`Run` fails before processing starts when a dedup rule has:

- empty `Name`,
- empty `Scope`,
- nil `Key`,
- invalid scope format (not `global` or `stage:<name>`),
- unknown stage in `stage:<name>`.

## Semantics

- Dedup check happens before enqueueing a stage job.
- Duplicate items are dropped (no enqueue) for that rule scope.
- Keys are stage-namespaced (`<stage>\x00<key>`) for collision safety.

## Example

```go
res, err := p.Run(
	context.Background(),
	map[string][]Item{"discover": seeds},
	pipex.WithDedupRules[Item](
		pipex.DedupRule[Item]{
			Name:  "hosts",
			Scope: pipex.DedupScopeStage("http_probe"),
			Key: func(it Item) string {
				return it.Host
			},
		},
	),
)
```

## Notes

- `TTL` is currently reserved for future behavior and is not enforced yet.
