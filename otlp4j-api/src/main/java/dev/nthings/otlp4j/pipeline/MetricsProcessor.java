package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.MetricsData;

/// A [Processor] specialised for metric batches - the SAM user code plugs a metric-processing lambda into.
@FunctionalInterface
public interface MetricsProcessor extends Processor<MetricsData> {}
