package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.SignalBatch;
import java.util.concurrent.CompletionStage;

/// Transport-side client used by exporters.
///
/// Asynchronous so callers can fan out without spawning threads themselves. Application code
/// normally uses `OtlpGrpcExporter` rather than this SPI directly.
public interface OtlpClient extends AutoCloseable {

    /// Exports one batch of `batch.signal()` and completes when the endpoint acknowledges.
    <T> CompletionStage<ConsumeResult> export(SignalBatch<T> batch);

    /// Releases the client's transport resources.
    @Override
    void close();
}
