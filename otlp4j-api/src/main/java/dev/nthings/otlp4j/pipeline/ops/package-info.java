/// Processing building blocks attached as pipeline consumers.
///
/// `Transforms` is the discoverable home for ready-made 1:1 `Transform`s (span and log-record
/// filters, plus per-signal resource-attribute setters). `Processors` builds stateless, named
/// same-signal `Processor`s (`pruning`, `of`), and `AbstractProcessor` is the base for stateful,
/// lifecycle-bearing ones. `Batcher` is a stateful, queue-backed batching `Processor` with a
/// configurable `OverflowPolicy`, usable mid-chain via `Stage.via(...)` or as a bound Sink terminal.
/// Signal-changing derivations live in the `Connectors` count sinks (`spanCount`, `logRecordCount`)
/// under a configurable `FailurePolicy`.
@NullMarked
package dev.nthings.otlp4j.pipeline.ops;

import org.jspecify.annotations.NullMarked;
