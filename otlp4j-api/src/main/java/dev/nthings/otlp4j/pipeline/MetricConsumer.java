package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.MetricsData;

/// A [Consumer] specialised for metric batches.
@FunctionalInterface
public interface MetricConsumer extends Consumer<MetricsData> {}
