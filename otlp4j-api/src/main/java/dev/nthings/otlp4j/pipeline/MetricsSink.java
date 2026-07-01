package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.MetricsData;

/// A [Sink] specialised for metric batches.
///
/// Build one from a lambda, or from a plain consumer or stage-returning function via
/// [Sink#accepting(Sink.ThrowingConsumer)] and [Sink#fromStage(java.util.function.Function)].
@FunctionalInterface
public interface MetricsSink extends Sink<MetricsData> {}
