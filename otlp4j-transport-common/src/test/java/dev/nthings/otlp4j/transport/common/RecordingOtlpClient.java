package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/// Test `OtlpClient` that accepts every signal and counts `shutdown()` calls; tests that don't care
/// about the count simply ignore it.
final class RecordingOtlpClient implements OtlpClient {

    final AtomicInteger closes = new AtomicInteger();

    @Override
    public CompletionStage<DeliveryResult> export(TelemetryPayload payload, Provenance provenance) {
        return DeliveryResult.acceptedStage(provenance);
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        closes.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }
}
