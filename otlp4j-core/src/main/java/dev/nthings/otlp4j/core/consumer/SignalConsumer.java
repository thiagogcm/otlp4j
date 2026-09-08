package dev.nthings.otlp4j.core.consumer;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import java.util.concurrent.CompletionStage;

/// A consumer of one signal's typed telemetry payload.
///
/// @param <T> the signal's domain batch type
public interface SignalConsumer<T> {

    /// Consumes one payload, returning a stage that completes with the delivery result. The result
    /// carries the consumer's provenance and the retry, reason, and downstream disposition of the
    /// outcome.
    ///
    /// @param context the per-invocation delivery context
    /// @param data    the typed payload to consume
    /// @return a stage completing with the delivery result
    CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data);
}
