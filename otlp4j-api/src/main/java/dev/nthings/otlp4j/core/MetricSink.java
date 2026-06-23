package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.MetricsData;

/// A [Sink] specialised for metric batches.
@FunctionalInterface
public interface MetricSink extends Sink<MetricsData> {}
