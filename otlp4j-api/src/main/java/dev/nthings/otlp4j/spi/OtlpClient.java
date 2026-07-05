package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.SignalBatch;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// Transport-side client used by exporters.
///
/// Asynchronous so callers can fan out without spawning threads themselves. Application code
/// normally uses `OtlpGrpcExporter` rather than this SPI directly.
///
/// Symmetric with [OtlpServer]: both are [Lifecycle] resources, so a client drains through the same
/// deadline-bounded [#shutdown(Duration)] the rest of the pipeline uses, and inherits the
/// swallow-and-warn [Lifecycle#close()] (a throw during try-with-resources teardown would mask the
/// primary exception). [#forceFlush(Duration)] is the inherited no-op: a client does not buffer.
public interface OtlpClient extends Lifecycle {

    /// Exports one batch of `batch.signal()` and completes when the endpoint acknowledges.
    ///
    /// @param batch the batch to export
    /// @param <T>   the batch's signal payload type
    /// @return a stage completing with the export outcome
    <T> CompletionStage<ConsumeResult> export(SignalBatch<T> batch);

    /// Drains in-flight exports and releases the client's transport resources within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes when teardown finishes
    @Override
    CompletionStage<Void> shutdown(Duration timeout);
}
