package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.MetricsData;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// A [Sink] specialised for metric batches.
@FunctionalInterface
public interface MetricSink extends Sink<MetricsData> {

    /// A metric sink that accepts on normal return and rejects on a thrown exception.
    /// See [Sink#accepting(ThrowingConsumer)].
    static MetricSink accepting(ThrowingConsumer<? super MetricsData> action) {
        Sink<MetricsData> sink = Sink.accepting(action);
        return sink::consume;
    }

    /// A metric sink driven by a `CompletionStage<Void>`-returning action.
    /// See [Sink#fromStage(Function)].
    static MetricSink fromStage(Function<? super MetricsData, ? extends CompletionStage<Void>> action) {
        Sink<MetricsData> sink = Sink.fromStage(action);
        return sink::consume;
    }
}
