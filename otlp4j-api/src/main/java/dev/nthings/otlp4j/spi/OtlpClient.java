package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;

/// Transport-side client used by `OtlpGrpcExporter`.
///
/// Application code normally uses `OtlpGrpcExporter` instead of this SPI directly.
public interface OtlpClient extends AutoCloseable {

    ExportResult exportTraces(TraceData traces);

    ExportResult exportMetrics(MetricsData metrics);

    ExportResult exportLogs(LogsData logs);

    ExportResult exportProfiles(ProfilesData profiles);

    /// Releases the client's transport resources.
    @Override
    void close();
}
