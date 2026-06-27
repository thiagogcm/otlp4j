package dev.nthings.otlp4j.core;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A handle that owns a wired-up pipeline graph.
///
/// Closing a subscription detaches the consumer from its source and drains
/// lifecycle resources registered explicitly via `Stage.owns(...)` or the
/// two-arg `Stage.to(terminal, owner)`. Terminals that implement
/// [AutoCloseable] (such as a directly attached
/// [dev.nthings.otlp4j.processor.BatchingProcessor]) are also collected
/// automatically.
///
/// As a [Drainable], [#close()] performs a best-effort synchronous drain with a
/// default timeout and [#shutdown(Duration)] returns a stage that completes when
/// the drain finishes or the timeout elapses. As a
/// [Flushable], [#forceFlush(Duration)] pushes buffered batches downstream
/// without tearing the subscription down.
public interface Subscription extends Drainable, Flushable {

    /// Drains all in-flight batches and releases resources, returning a stage that
    /// completes successfully on a clean drain and exceptionally if the
    /// timeout elapses.
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// Flushes any buffered batches downstream without tearing the
    /// subscription down.
    @Override
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }
}
