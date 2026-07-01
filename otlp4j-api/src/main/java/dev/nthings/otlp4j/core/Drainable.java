package dev.nthings.otlp4j.core;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// A resource with a graceful, deadline-bounded asynchronous shutdown.
///
/// [#shutdown(Duration)] drains in-flight work and releases resources, completing successfully on a
/// clean drain and exceptionally if the deadline elapses. [PipelineHandle] and the framework's
/// terminals (exporter, batching processor, receiver) are all [Drainable], so a pipeline can drain
/// everything it collects within one shared budget.
public interface Drainable extends AutoCloseable {

    /// Drains the resource within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes on clean drain or exceptionally on timeout
    CompletionStage<Void> shutdown(Duration timeout);

    /// Drains with a default 10-second deadline.
    default CompletionStage<Void> shutdown() {
        return shutdown(Duration.ofSeconds(10));
    }

    /// Drains with a default 10-second deadline.
    @Override
    default void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }
}
