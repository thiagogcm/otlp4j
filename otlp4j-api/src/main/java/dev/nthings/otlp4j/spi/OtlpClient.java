package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.concurrent.CompletionStage;

/// Transport-side client used by exporters.
///
/// Asynchronous so callers can fan out without spawning threads themselves. Application code
/// normally uses `OtlpGrpcExporter` rather than this SPI directly.
public interface OtlpClient extends AutoCloseable {

    CompletionStage<ConsumeResult<TracesData>>    exportTraces(TracesData traces);

    CompletionStage<ConsumeResult<MetricsData>>  exportMetrics(MetricsData metrics);

    CompletionStage<ConsumeResult<LogsData>>     exportLogs(LogsData logs);

    CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData profiles);

    /// Releases the client's transport resources.
    @Override
    void close();
}
