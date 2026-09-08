package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;

/// A processor: a component that transforms one signal's telemetry in flight and forwards it to the
/// next consumer. It is both a `Component` (it has the lifecycle) and a `SignalConsumer` (it
/// consumes and forwards), matching the Collector processor, which is a lifecycle-bearing consumer.
///
/// @param <T> the signal's domain batch type this processor operates on
public interface Processor<T> extends Component, SignalConsumer<T> {
}
