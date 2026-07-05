package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.SignalBatch;
import java.util.concurrent.CompletionStage;

/// Dispatch entry point an [OtlpServer] invokes as it decodes incoming OTLP requests. Returns a
/// [CompletionStage] so the transport can hold the request open until the pipeline acknowledges
/// (or partial-succeeds).
public interface Dispatchers {

    /// Dispatches a decoded batch into the pipeline, returning the pipeline's ack.
    <T> CompletionStage<ConsumeResult> dispatch(SignalBatch<T> batch);
}
