package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.ProfilesData;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// A [Sink] specialised for profile batches.
@FunctionalInterface
public interface ProfileSink extends Sink<ProfilesData> {

    /// A profile sink that accepts on normal return and rejects on a thrown exception.
    /// See [Sink#accepting(ThrowingConsumer)].
    static ProfileSink accepting(ThrowingConsumer<? super ProfilesData> action) {
        Sink<ProfilesData> sink = Sink.accepting(action);
        return sink::consume;
    }

    /// A profile sink driven by a `CompletionStage<Void>`-returning action.
    /// See [Sink#fromStage(Function)].
    static ProfileSink fromStage(Function<? super ProfilesData, ? extends CompletionStage<Void>> action) {
        Sink<ProfilesData> sink = Sink.fromStage(action);
        return sink::consume;
    }
}
