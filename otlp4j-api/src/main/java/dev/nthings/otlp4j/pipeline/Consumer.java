package dev.nthings.otlp4j.pipeline;

import java.util.concurrent.CompletionStage;

/// A typed, asynchronous sink for one OTLP signal.
///
/// The four signals expose their own SAMs — [TraceConsumer], [MetricConsumer], [LogConsumer],
/// [ProfileConsumer] — which is what user code plugs lambdas into. Extending `Consumer<T>`
/// directly is useful only when a single type parameter is genuinely needed.
///
/// @param <T> the OTLP signal carried by this consumer
public interface Consumer<T> {

    /// Accepts one batch and returns a stage that completes when the batch has been processed.
    CompletionStage<ConsumeResult<T>> consume(T batch);
}
