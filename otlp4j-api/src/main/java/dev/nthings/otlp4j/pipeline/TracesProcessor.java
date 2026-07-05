package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.TracesData;

/// A [Processor] specialised for trace batches - the SAM user code plugs a trace-processing lambda into.
@FunctionalInterface
public interface TracesProcessor extends Processor<TracesData> {}
