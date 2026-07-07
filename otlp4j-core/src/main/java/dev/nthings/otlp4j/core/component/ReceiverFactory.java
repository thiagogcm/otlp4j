package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumers;

/// A factory for receivers. It creates one receiver instance from all its routed consumers at once,
/// because a per-signal creation would instantiate the OTLP receiver once per signal and four
/// instances cannot share one port. The `SignalConsumers` bundle carries the head consumer of every
/// pipeline the receiver feeds.
///
/// @param <C> the typed configuration this factory accepts
public interface ReceiverFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one receiver serving every signal in the consumer bundle.
    ///
    /// @param ctx       the create context (identity and base context)
    /// @param config    the receiver configuration
    /// @param consumers the routed consumers, one per served signal
    /// @return the receiver
    Receiver create(CreateContext ctx, C config, SignalConsumers consumers);
}
