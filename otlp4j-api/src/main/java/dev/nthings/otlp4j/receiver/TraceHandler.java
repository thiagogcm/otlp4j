package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;

/// Consumes trace telemetry delivered to an [OtlpReceiver].
///
/// Return an [ExportResult] to acknowledge the batch; throw to fail at the transport level.
@FunctionalInterface
public interface TraceHandler {

    ExportResult onTraces(TraceData traces);
}
