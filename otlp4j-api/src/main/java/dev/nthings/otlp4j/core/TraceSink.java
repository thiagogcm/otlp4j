package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.TraceData;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// A [Sink] specialised for trace batches.
@FunctionalInterface
public interface TraceSink extends Sink<TraceData> {

    /// A trace sink that accepts on normal return and rejects on a thrown exception.
    /// See [Sink#accepting(ThrowingConsumer)].
    static TraceSink accepting(ThrowingConsumer<? super TraceData> action) {
        Sink<TraceData> sink = Sink.accepting(action);
        return sink::consume;
    }

    /// A trace sink driven by a `CompletionStage<Void>`-returning action.
    /// See [Sink#fromStage(Function)].
    static TraceSink fromStage(Function<? super TraceData, ? extends CompletionStage<Void>> action) {
        Sink<TraceData> sink = Sink.fromStage(action);
        return sink::consume;
    }
}
