package dev.nthings.otlp4j.core;

/// A side-effecting consumer of one signal batch that is allowed to throw.
///
/// This is the lambda shape passed to [Sink#accepting(ThrowingConsumer)] and the per-signal
/// `accepting(...)` factories. Returning normally signals acceptance; any thrown exception is
/// mapped to a rejected [dev.nthings.otlp4j.model.ConsumeResult]. It exists so simple handlers can
/// be written without manually returning `ConsumeResult.acceptedStage()`.
///
/// @param <T> the OTLP signal carried by this consumer
@FunctionalInterface
public interface ThrowingConsumer<T> {

    void accept(T batch) throws Exception;
}
