package dev.nthings.otlp4j.core.consumer;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletionStage;

/// A consumer of one signal's telemetry, replacing the legacy `Sink` and its four per-signal
/// variants. A `SignalConsumer<T>` typed through a `SignalDescriptor<T>` supplies the same type
/// safety the four per-signal interfaces gave without four interfaces, because the payload type `T`
/// is bound to the descriptor.
///
/// @param <T> the payload type this consumer accepts
public interface SignalConsumer<T extends TelemetryPayload> {

    /// The consumer's declared capabilities, read before delivery so the fan-out node can copy ahead
    /// of a mutating consumer.
    ///
    /// @return the capabilities
    ConsumerCapabilities capabilities();

    /// Consumes one payload, returning a stage that completes with the delivery result. The result
    /// carries the consumer's provenance and the retry, reason, and downstream disposition of the
    /// outcome.
    ///
    /// @param context the per-invocation delivery context
    /// @param data    the payload to consume
    /// @return a stage completing with the delivery result
    CompletionStage<DeliveryResult> consume(ConsumeContext context, T data);

    /// Reinterprets a consumer of any signal's payload as one keyed by the opaque [TelemetryPayload],
    /// the single sanctioned unchecked cast a component factory needs to wire its component onto the
    /// graph's payload-typed downstream consumer. At runtime the consumer is agnostic to the concrete
    /// payload backing, so the reinterpretation is safe.
    ///
    /// @param consumer the downstream consumer
    /// @return the same consumer typed over [TelemetryPayload]
    @SuppressWarnings("unchecked")
    static SignalConsumer<TelemetryPayload> asPayload(SignalConsumer<?> consumer) {
        return (SignalConsumer<TelemetryPayload>) consumer;
    }
}
