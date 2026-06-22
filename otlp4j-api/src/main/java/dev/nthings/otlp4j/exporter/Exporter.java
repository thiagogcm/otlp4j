package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.Drainable;
import dev.nthings.otlp4j.pipeline.Flushable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Terminal [Consumer] with a lifecycle.
///
/// Implement to send telemetry to a custom destination; see [OtlpGrpcExporter] for the built-in
/// OTLP/gRPC exporter. As a [Drainable] and [Flushable] an exporter participates in pipeline
/// shutdown and `forceFlush` once registered via `Stage.owns(...)`; both lifecycle hooks default to
/// no-ops so a minimal exporter need only implement `consume`.
public interface Exporter<T> extends Consumer<T>, Drainable, Flushable {

    /// Flushes any in-flight batches downstream. The default does nothing.
    @Override
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Releases the exporter's resources. The default does nothing.
    @Override
    default CompletionStage<Void> shutdown(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }
}
