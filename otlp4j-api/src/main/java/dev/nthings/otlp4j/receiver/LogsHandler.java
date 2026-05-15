package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.pipeline.ExportResult;

/// Consumes log telemetry delivered to an [OtlpReceiver].
///
/// Return an [ExportResult] to acknowledge the batch; throw to fail at the transport level.
@FunctionalInterface
public interface LogsHandler {

    ExportResult onLogs(LogsData logs);
}
