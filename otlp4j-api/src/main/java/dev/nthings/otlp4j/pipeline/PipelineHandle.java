package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// A handle that owns a wired-up pipeline graph.
///
/// Closing a subscription detaches the consumer from its source and drains its
/// lifecycle resources: terminals and fan-out peers that implement [AutoCloseable]
/// (such as an exporter facet or a directly attached `Batcher`) are
/// collected automatically, along with any resource registered via [Pipeline.Stage#owns].
/// Collected resources are deduplicated by reference identity, so a resource reachable
/// twice (owned and auto-collected, or a duplicated fan-out peer) is retained once and
/// shut down once.
///
/// As a [Lifecycle], [#close()] performs a best-effort synchronous drain with a
/// default timeout, [#shutdown(Duration)] returns a stage that completes when the
/// drain finishes or the timeout elapses, and [#forceFlush(Duration)] pushes
/// buffered batches downstream without tearing the subscription down.
public interface PipelineHandle extends Lifecycle {

    /// Drains in-flight batches and releases resources.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes on clean drain or exceptionally on timeout
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// A human-readable dump of this pipeline's visible topology (source, terminal shape, and the
    /// [AutoCloseable] resources it will drain). A custom handle that cannot introspect itself
    /// returns an opaque one-liner. Cheap; intended for logs and tests, not the ingest path.
    ///
    /// @return a human-readable topology description
    default String describe() {
        return "pipeline<opaque handle: " + getClass().getName() + ">";
    }

    /// This handle's reflection-free structural topology, consumed by [Wiring#validate] and
    /// [Wiring#describe]. A custom handle returns [PipelineTopology#opaque()] (the safe default) so
    /// third-party handles never break validation.
    ///
    /// @return this handle's declared topology, opaque for a custom handle
    default PipelineTopology topology() {
        return PipelineTopology.opaque();
    }
}
