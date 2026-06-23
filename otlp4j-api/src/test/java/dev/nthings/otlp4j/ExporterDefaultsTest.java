package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.exporter.Exporter;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Covers the default-method paths on the [Exporter] interface without bringing up a transport.
@DisplayName("Exporter default methods")
class ExporterDefaultsTest {

    @DisplayName("forceFlush is a no-op and close() invokes shutdown")
    @Test
    void defaultsAreSafeNoOpsThatCloseInvokesShutdown() {
        var consumed = new AtomicBoolean();
        var shutdownCalled = new AtomicBoolean();
        var exporter = new Exporter<TraceData>() {
            @Override
            public CompletionStage<ConsumeResult<TraceData>> consume(TraceData batch) {
                consumed.set(true);
                return ConsumeResult.acceptedStage();
            }
            @Override
            public CompletionStage<Void> shutdown(Duration timeout) {
                shutdownCalled.set(true);
                return CompletableFuture.completedFuture(null);
            }
        };
        exporter.consume(new TraceData(List.of())).toCompletableFuture().join();
        exporter.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        exporter.close();
        assertThat(consumed.get()).isTrue();
        assertThat(shutdownCalled.get()).isTrue();
    }

    @DisplayName("Default forceFlush and shutdown complete normally")
    @Test
    void defaultForceFlushAndShutdownComplete() {
        Exporter<TraceData> exporter = batch -> ConsumeResult.acceptedStage();
        exporter.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        exporter.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        exporter.close();
    }
}
