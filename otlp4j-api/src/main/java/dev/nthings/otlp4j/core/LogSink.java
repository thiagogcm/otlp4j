package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.LogsData;

/// A [Sink] specialised for log batches.
@FunctionalInterface
public interface LogSink extends Sink<LogsData> {}
