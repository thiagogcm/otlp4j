package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.concurrent.CompletionStage;

/// A typed, asynchronous sink for one OTLP signal.
///
/// The four signals expose their own SAMs — [TraceSink], [MetricSink], [LogSink],
/// [ProfileSink] — which is what user code plugs lambdas into. Extending `Sink<T>`
/// directly is useful only when a single type parameter is genuinely needed.
///
/// @param <T> the OTLP signal carried by this sink
public interface Sink<T> {

    /// Accepts one batch and returns a stage that completes when the batch has been processed.
    CompletionStage<ConsumeResult<T>> consume(T batch);
}
