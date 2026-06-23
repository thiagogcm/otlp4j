/// Core signal-handling vocabulary shared across the API.
///
/// A [Sink] is the typed asynchronous terminal for one signal — the per-signal SAMs [TraceSink],
/// [MetricSink], [LogSink], [ProfileSink] are what user code plugs lambdas into. A [Source] is where
/// a sink attaches, and the resulting wiring is owned by a [Subscription]. [Drainable] and
/// [Flushable] are the shared lifecycle hooks, and [Telemetry] is the sealed four-signal envelope.
package dev.nthings.otlp4j.core;
