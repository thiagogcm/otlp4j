package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// A resource with a graceful, deadline-bounded asynchronous shutdown.
///
/// [#shutdown(Duration)] drains in-flight work and releases resources, completing successfully on a
/// clean drain and exceptionally if the deadline elapses. [Subscription] and the framework's
/// terminals (exporter, batching processor, receiver) are all `Drainable`, so a pipeline can drain
/// everything it owns within one shared budget — see [Pipeline.Stage#owns(AutoCloseable)].
public interface Drainable extends AutoCloseable {

    /// Drains the resource within `timeout`, completing exceptionally if it elapses.
    CompletionStage<Void> shutdown(Duration timeout);

    /// Gracefully drains with a default 10-second deadline.
    @Override
    default void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }
}
