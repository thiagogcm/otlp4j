/// The pipeline DSL: how telemetry flows from a `Source` through stages to a `Sink`.
///
/// `Pipeline.from(source)` opens a fluent chain of `transform`/`filter` stages, optionally
/// fans out via `branch().fanOut(...).join()` or `FanOut`, and terminates at a `Sink` that
/// returns a `ConsumeResult` (`Accepted`/`Partial`/`Rejected`). The returned `PipelineHandle` owns
/// the wiring — closing it detaches the source and drains attached lifecycle resources. Prefer the
/// per-signal consumer aliases `TraceSink`, `MetricSink`, `LogSink`, `ProfileSink`.
@NullMarked
package dev.nthings.otlp4j.pipeline;

import org.jspecify.annotations.NullMarked;
