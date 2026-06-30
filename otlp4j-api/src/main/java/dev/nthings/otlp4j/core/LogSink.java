package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.LogsData;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// A [Sink] specialised for log batches.
@FunctionalInterface
public interface LogSink extends Sink<LogsData> {

    /// A log sink that accepts on normal return and rejects on a thrown exception.
    /// See [Sink#accepting(ThrowingConsumer)].
    ///
    /// @param action the synchronous action
    /// @return a new [LogSink]
    static LogSink accepting(ThrowingConsumer<? super LogsData> action) {
        Sink<LogsData> sink = Sink.accepting(action);
        return sink::consume;
    }

    /// A log sink driven by a [CompletionStage]-returning action.
    /// See [Sink#fromStage(Function)].
    ///
    /// @param action the async action
    /// @return a new [LogSink]
    static LogSink fromStage(Function<? super LogsData, ? extends CompletionStage<Void>> action) {
        Sink<LogsData> sink = Sink.fromStage(action);
        return sink::consume;
    }
}
