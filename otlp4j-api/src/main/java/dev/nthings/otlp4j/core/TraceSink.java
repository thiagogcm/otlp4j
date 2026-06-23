package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.TraceData;

/// A [Sink] specialised for trace batches.
@FunctionalInterface
public interface TraceSink extends Sink<TraceData> {}
