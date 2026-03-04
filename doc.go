// Package pipex provides a generic pipeline for staged item processing.
//
// A pipeline is a directed acyclic graph of stages. Each stage processes one
// input item and emits zero or more output items. Emitted items are routed to
// downstream stages defined by Connect edges.
//
// Data can enter the pipeline through:
//   - static seeds passed to Run
//   - dynamic triggers registered with WithTriggers
//
// Data can leave the pipeline through:
//   - returned Run results (per-stage accumulated outputs)
//   - sinks registered with WithSinks (per-item egress side effects)
//
// Sink retries are configured with WithSinkRetry(maxRetries, backoff).
//
// See runnable examples in:
//   - examples/quickstart
//   - examples/sink
package pipex

