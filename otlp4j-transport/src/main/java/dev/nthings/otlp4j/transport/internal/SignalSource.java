package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// The per-signal consumer slot a [ServerReceiver] routes one signal's decoded payloads through.
///
/// A single graph consumer is attached at wiring time; a second attach throws. A decoded payload is
/// forwarded verbatim to the attached consumer, whose [DeliveryResult] flows back to the transport
/// unchanged. When no consumer is attached, dispatch answers a retryable [DeliveryReason#NO_CONSUMER]
/// rejection attributed to the receiver, which the transport renders as gRPC `UNAVAILABLE` / HTTP
/// `503`. A directly attached consumer's throw or failed stage is NOT normalized here: it propagates
/// so the transport renders gRPC `INTERNAL` / HTTP `500`, exactly as the graph boundary upstream
/// leaves it.
///
/// @param <T> the domain batch type this signal identifies
public final class SignalSource<T> {

    private final SignalDescriptor<T> signal;
    private final Provenance provenance;
    private final DeliveryResult noConsumerResult;
    private final AtomicReference<@Nullable SignalConsumer<TelemetryPayload>> attached = new AtomicReference<>();

    public SignalSource(SignalDescriptor<T> signal, ComponentId receiverId) {
        this.signal = Objects.requireNonNull(signal, "signal");
        this.provenance = new Provenance(receiverId, ComponentKind.RECEIVER, signal);
        this.noConsumerResult = new DeliveryResult.Rejected(
                Disposition.RETRYABLE,
                DeliveryReason.NO_CONSUMER,
                "no consumer attached for " + signal.batchType().getSimpleName()
                        + "; route a consumer to accept it",
                null,
                null,
                false,
                provenance);
    }

    /// Attaches the single graph consumer for this signal. A second attach throws.
    ///
    /// @param consumer the graph consumer routed for this signal
    public void attach(SignalConsumer<TelemetryPayload> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (!attached.compareAndSet(null, consumer)) {
            throw new IllegalStateException(
                    "source for " + signal.batchType().getSimpleName() + " already has a consumer");
        }
    }

    /// Forwards `payload` to the attached consumer, or answers the no-consumer rejection.
    ///
    /// @param context the per-invocation delivery context
    /// @param payload the decoded payload
    /// @return a stage completing with the delivery result
    public CompletionStage<DeliveryResult> dispatch(ConsumeContext context, TelemetryPayload payload) {
        var consumer = attached.get();
        if (consumer == null) {
            return CompletableFuture.completedFuture(noConsumerResult);
        }
        return consumer.consume(context, payload);
    }
}
