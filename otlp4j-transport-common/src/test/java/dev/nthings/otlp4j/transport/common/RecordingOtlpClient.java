package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.SignalBatch;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/// Test `OtlpClient` that accepts every signal and counts `close()` calls; tests that don't care
/// about the count simply ignore it.
final class RecordingOtlpClient implements OtlpClient {

    final AtomicInteger closes = new AtomicInteger();

    @Override
    public <T> CompletionStage<ConsumeResult> export(SignalBatch<T> batch) {
        return ConsumeResult.acceptedStage();
    }

    @Override
    public void close() {
        closes.incrementAndGet();
    }
}
