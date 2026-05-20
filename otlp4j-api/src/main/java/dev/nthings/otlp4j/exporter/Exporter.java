package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.pipeline.Consumer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Terminal [Consumer] with a lifecycle.
///
/// Implement to send telemetry to a custom destination; see [OtlpGrpcExporter] for the built-in
/// OTLP/gRPC exporter. Lifecycle methods return [CompletionStage] so exporters can participate
/// in pipeline shutdown without blocking.
public interface Exporter<T> extends Consumer<T>, AutoCloseable {

    /// Flushes any in-flight batches downstream. The default does nothing.
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Releases the exporter's resources. The default does nothing.
    default CompletionStage<Void> shutdown(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    default void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }
}
