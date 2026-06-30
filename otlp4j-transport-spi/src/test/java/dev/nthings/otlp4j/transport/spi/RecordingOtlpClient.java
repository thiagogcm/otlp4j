package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/// Test `OtlpClient` that accepts every signal and counts `close()` calls; tests that don't care
/// about the count simply ignore it.
final class RecordingOtlpClient implements OtlpClient {

    final AtomicInteger closes = new AtomicInteger();

    @Override
    public CompletionStage<ConsumeResult<TracesData>> exportTraces(TracesData t) {
        return ConsumeResult.acceptedStage();
    }

    @Override
    public CompletionStage<ConsumeResult<MetricsData>> exportMetrics(MetricsData m) {
        return ConsumeResult.acceptedStage();
    }

    @Override
    public CompletionStage<ConsumeResult<LogsData>> exportLogs(LogsData l) {
        return ConsumeResult.acceptedStage();
    }

    @Override
    public CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData p) {
        return ConsumeResult.acceptedStage();
    }

    @Override
    public void close() {
        closes.incrementAndGet();
    }
}
