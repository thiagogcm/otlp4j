package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// A processor: a component that transforms one signal's telemetry in flight and forwards it to the
/// next consumer. It is both a `Component` (it has the lifecycle) and a `SignalConsumer` (it
/// consumes and forwards), matching the Collector processor, which is a lifecycle-bearing consumer.
///
/// @param <T> the payload type this processor operates on
public interface Processor<T extends TelemetryPayload> extends Component, SignalConsumer<T> {
}
