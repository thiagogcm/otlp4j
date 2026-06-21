package dev.nthings.otlp4j.pipeline;

/// A typed signal source — a place a [Consumer] can attach to receive batches.
///
/// Sources are produced by receivers (one per signal) and by graph builder nodes. Implementations
/// guarantee that at most one consumer is attached at a time; for multi-consumer fan-out use
/// [FanOut] explicitly. Lifecycle is owned by the returned [Subscription].
///
/// @param <T> the OTLP signal carried by this source
public interface Source<T> {

    /// Attaches `consumer` and returns a subscription whose [Subscription#close()] detaches it.
    Subscription subscribe(Consumer<? super T> consumer);
}
