package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.LogsData;

/// A [Sink] specialised for log batches.
///
/// Build one from a lambda, or from a plain consumer or stage-returning function via
/// [Sink#accepting(ThrowingConsumer)] and [Sink#fromStage(java.util.function.Function)].
@FunctionalInterface
public interface LogsSink extends Sink<LogsData> {}
