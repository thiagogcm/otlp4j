package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.ConsumeResult;

/// A typed signal source — a place a [Sink] can attach to receive batches.
///
/// Sources are produced by receivers (one per signal) and by graph builder nodes. Implementations
/// guarantee that at most one sink is attached at a time; for multi-sink fan-out use
/// `FanOut` explicitly. Lifecycle is owned by the returned [PipelineHandle].
///
/// @param <T> the OTLP signal carried by this source
public interface Source<T> {

    /// Attaches `consumer` and returns a subscription whose [PipelineHandle#close()] detaches it.
    PipelineHandle subscribe(Sink<? super T> consumer);

    /// Explicitly accepts and drops this signal until the returned subscription is closed.
    ///
    /// This occupies the same single attachment slot as [#subscribe(Sink)] and is intended for
    /// signals the receiver should acknowledge even though the application has deliberately chosen
    /// not to process them.
    default PipelineHandle discard() {
        return subscribe(_ -> ConsumeResult.acceptedStage());
    }
}
