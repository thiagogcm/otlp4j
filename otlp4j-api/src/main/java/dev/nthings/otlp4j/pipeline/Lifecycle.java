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

    /// The grace period [#close()] and the no-argument [#shutdown()]/[#forceFlush()] drain within:
    /// ten seconds, applied uniformly by every `Lifecycle` (receiver, exporter, batching processor,
    /// subscription).
    Duration DEFAULT_GRACE_PERIOD = Duration.ofSeconds(10);

    /// Drains the resource within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes on clean drain or exceptionally on timeout
    CompletionStage<Void> shutdown(Duration timeout);

    /// Drains with the [#DEFAULT_GRACE_PERIOD].
    ///
    /// @return a stage that completes on clean drain or exceptionally on timeout
    default CompletionStage<Void> shutdown() {
        return shutdown(DEFAULT_GRACE_PERIOD);
    }

    /// Flushes buffered batches downstream without tearing the resource down. The default does nothing.
    ///
    /// @param timeout the flush deadline
    /// @return a stage that completes when flushed
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Flushes with the [#DEFAULT_GRACE_PERIOD].
    ///
    /// @return a stage that completes when flushed
    default CompletionStage<Void> forceFlush() {
        return forceFlush(DEFAULT_GRACE_PERIOD);
    }

    /// Drains with the [#DEFAULT_GRACE_PERIOD].
    @Override
    default void close() {
        shutdown(DEFAULT_GRACE_PERIOD).toCompletableFuture().join();
    }
}
