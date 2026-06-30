package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.TracesData;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// A [Sink] specialised for trace batches.
@FunctionalInterface
public interface TraceSink extends Sink<TracesData> {

    /// A trace sink that accepts on normal return and rejects on a thrown exception.
    /// See [Sink#accepting(ThrowingConsumer)].
    ///
    /// @param action the synchronous action
    /// @return a new [TraceSink]
    static TraceSink accepting(ThrowingConsumer<? super TracesData> action) {
        Sink<TracesData> sink = Sink.accepting(action);
        return sink::consume;
    }

    /// A trace sink driven by a [CompletionStage]-returning action.
    /// See [Sink#fromStage(Function)].
    ///
    /// @param action the async action
    /// @return a new [TraceSink]
    static TraceSink fromStage(Function<? super TracesData, ? extends CompletionStage<Void>> action) {
        Sink<TracesData> sink = Sink.fromStage(action);
        return sink::consume;
    }
}
