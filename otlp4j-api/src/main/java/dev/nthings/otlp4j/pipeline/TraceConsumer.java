package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.TraceData;

/// A [Consumer] specialised for trace batches.
@FunctionalInterface
public interface TraceConsumer extends Consumer<TraceData> {}
