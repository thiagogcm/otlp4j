package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.LogsData;

/// A [Consumer] specialised for log batches.
@FunctionalInterface
public interface LogConsumer extends Consumer<LogsData> {}
