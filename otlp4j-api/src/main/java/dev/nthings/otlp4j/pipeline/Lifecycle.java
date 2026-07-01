package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A resource with a graceful, deadline-bounded lifecycle.
///
/// [#shutdown(Duration)] drains in-flight work and releases resources, completing successfully on a
/// clean drain and exceptionally if the deadline elapses. [#forceFlush(Duration)] pushes buffered
/// batches downstream without tearing the resource down; it is a no-op unless the resource buffers
/// (only `BatchingProcessor` does). [PipelineHandle] and the framework's terminals (exporter,
/// batching processor, receiver) are all [Lifecycle], so a pipeline can drain and flush everything
/// it collects within one shared budget.
public interface Lifecycle extends AutoCloseable {

    /// Drains the resource within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes on clean drain or exceptionally on timeout
    CompletionStage<Void> shutdown(Duration timeout);

    /// Drains with a default 10-second deadline.
    ///
    /// @return a stage that completes on clean drain or exceptionally on timeout
    default CompletionStage<Void> shutdown() {
        return shutdown(Duration.ofSeconds(10));
    }

    /// Flushes buffered batches downstream without tearing the resource down. The default does nothing.
    ///
    /// @param timeout the flush deadline
    /// @return a stage that completes when flushed
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Flushes with a default 10-second deadline.
    ///
    /// @return a stage that completes when flushed
    default CompletionStage<Void> forceFlush() {
        return forceFlush(Duration.ofSeconds(10));
    }

    /// Drains with a default 10-second deadline.
    @Override
    default void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }
}
