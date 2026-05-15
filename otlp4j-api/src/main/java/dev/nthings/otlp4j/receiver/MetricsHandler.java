package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.pipeline.ExportResult;

/// Consumes metric telemetry delivered to an [OtlpReceiver].
///
/// Return an [ExportResult] to acknowledge the batch; throw to fail at the transport level.
@FunctionalInterface
public interface MetricsHandler {

    ExportResult onMetrics(MetricsData metrics);
}
