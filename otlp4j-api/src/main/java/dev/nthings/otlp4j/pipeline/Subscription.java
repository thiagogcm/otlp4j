package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A handle that owns a wired-up pipeline graph.
///
/// Closing a subscription detaches the consumer from its source and drains any lifecycle resources
/// the chain owns — both those auto-collected from the terminal (e.g. fan-out peers that implement
/// `AutoCloseable`) and those registered explicitly via `Stage.owns(...)` for resources hidden
/// behind method-reference consumers. All resources share a single shutdown deadline. Two flavours
/// are available:
///
///   - [#close()] performs a best-effort synchronous drain using a sensible default timeout.
///   - [#shutdown(Duration)] returns a stage that completes when drain is finished or the timeout
///     elapses.
public interface Subscription extends Pipeline.Drainable {

    /// Drains all in-flight batches and releases resources, returning a stage that completes
    /// successfully on a clean drain and exceptionally if the timeout elapses.
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// Flushes any buffered batches downstream without tearing the subscription down.
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Tears the subscription down with a default 10-second timeout.
    @Override
    default void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }
}
