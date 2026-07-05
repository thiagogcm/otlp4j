package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.LogsData;

/// A [Processor] specialised for log batches - the SAM user code plugs a log-processing lambda into.
@FunctionalInterface
public interface LogsProcessor extends Processor<LogsData> {}
