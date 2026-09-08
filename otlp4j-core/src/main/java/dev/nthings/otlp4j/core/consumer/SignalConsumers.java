package dev.nthings.otlp4j.core.consumer;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Set;

/// The bundle of routed consumers a receiver factory receives, one per signal the receiver serves.
/// A receiver factory takes all its routed consumers at once, because a per-signal creation would
/// instantiate the OTLP receiver once per signal and four instances cannot share one port. The
/// bundle answers which signals it routes and hands back the typed consumer for a given signal.
public interface SignalConsumers {

    /// The signals this bundle routes.
    ///
    /// @return the set of routed signals
    Set<SignalDescriptor<?>> signals();

    /// The consumer for `signal`, typed through the descriptor. The signal must be a member of
    /// [#signals()].
    ///
    /// @param signal the signal to route
    /// @param <T>    the signal's domain batch type
    /// @return the consumer for that signal
    <T> SignalConsumer<T> consumer(SignalDescriptor<T> signal);
}
