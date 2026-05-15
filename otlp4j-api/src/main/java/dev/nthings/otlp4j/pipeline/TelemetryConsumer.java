package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;

/// A sink for typed telemetry and the base contract for pipeline components.
///
/// Receivers feed consumers, processors wrap consumers, connectors emit into consumers, and
/// exporters are terminal consumers. Methods default to [ExportResult#success()] so implementations
/// can override only the signals they handle.
public interface TelemetryConsumer {

    default ExportResult consumeTraces(TraceData traces) {
        return ExportResult.success();
    }

    default ExportResult consumeMetrics(MetricsData metrics) {
        return ExportResult.success();
    }

    default ExportResult consumeLogs(LogsData logs) {
        return ExportResult.success();
    }

    default ExportResult consumeProfiles(ProfilesData profiles) {
        return ExportResult.success();
    }
}
