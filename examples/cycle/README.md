# Cycle Example

Run:

```bash
go run ./examples/cycle
```

Expected output shape:

```text
no dedup:
  a outputs: [...]
  b outputs: [...]
  a count: <larger number>
  b count: <larger number>
dedup by parity:
  a outputs: [...]
  b outputs: [...]
  a count: <smaller number>
  b count: <smaller number>
```

Notes:

- `no dedup` run is bounded by `maxHops`.
- `dedup by parity` drops revisits per `(stage, parity)` key, so fewer items are processed.
