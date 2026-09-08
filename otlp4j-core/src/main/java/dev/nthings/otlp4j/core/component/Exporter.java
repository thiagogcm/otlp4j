package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;

/// An exporter: a component that sends one signal's telemetry off the gateway. It is both a
/// `Component` (it has the lifecycle) and a `SignalConsumer` (the graph feeds it as the terminal
/// consumer of a pipeline), matching the Collector exporter, which is a lifecycle-bearing consumer.
///
/// @param <T> the signal's domain batch type this exporter operates on
public interface Exporter<T> extends Component, SignalConsumer<T> {
}
