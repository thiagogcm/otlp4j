package dev.nthings.otlp4j.core;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A handle that owns a wired-up pipeline graph.
///
/// Closing a subscription detaches the consumer from its source and drains its
/// lifecycle resources: terminals and fan-out peers that implement [AutoCloseable]
/// (such as an exporter facet or a directly attached [BatchingProcessor]) are
/// collected automatically, along with any resource registered via [Pipeline.Stage#owns].
///
/// As a [Drainable], [#close()] performs a best-effort synchronous drain with a
/// default timeout and [#shutdown(Duration)] returns a stage that completes when
/// the drain finishes or the timeout elapses. As a [ForceFlushable],
/// [#forceFlush(Duration)] pushes buffered batches downstream without tearing the
/// subscription down.
public interface PipelineHandle extends Drainable, ForceFlushable {

    /// Drains in-flight batches and releases resources.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes on clean drain or exceptionally on timeout
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// Flushes buffered batches downstream without tearing the subscription down.
    ///
    /// @param timeout the flush deadline
    /// @return a stage that completes when flushed
    @Override
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }
}
