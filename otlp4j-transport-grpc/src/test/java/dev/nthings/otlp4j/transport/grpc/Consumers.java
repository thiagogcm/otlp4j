package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.ModelBatchPayload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/// Test-only [SignalConsumer] factories that let the OTLP/gRPC transport tests route decoded payloads
/// with the same brevity the legacy per-signal sinks gave them.
final class Consumers {

    /// A generic sink provenance; the wire mapping never reads it, so one shared value suffices.
    static final Provenance PROV =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    private Consumers() {
    }

    /// A consumer applying `fn` to the payload and completing at once.
    static SignalConsumer<TelemetryPayload> of(Function<TelemetryPayload, DeliveryResult> fn) {
        return new SignalConsumer<>() {
            @Override
            public ConsumerCapabilities capabilities() {
                return ConsumerCapabilities.nonMutating();
            }

            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
                return CompletableFuture.completedFuture(fn.apply(data));
            }
        };
    }

    /// A consumer that accepts every payload.
    static SignalConsumer<TelemetryPayload> accepting() {
        return of(p -> DeliveryResult.accepted(PROV));
    }

    /// A consumer that records the decoded batch of `signal` and accepts.
    static <T> SignalConsumer<TelemetryPayload> record(SignalDescriptor<T> signal, AtomicReference<? super T> sink) {
        return of(p -> {
            sink.set(signal.batchType().cast(((ModelBatchPayload<?>) p).batch()));
            return DeliveryResult.accepted(PROV);
        });
    }

    /// A consumer reporting a partial-success rejection of `rejected` items.
    static SignalConsumer<TelemetryPayload> partial(long rejected, String message) {
        return of(p -> DeliveryResult.partial(rejected, message, PROV));
    }

    /// A consumer that throws `error` synchronously, exercising the transport error path.
    static SignalConsumer<TelemetryPayload> throwing(RuntimeException error) {
        return of(p -> {
            throw error;
        });
    }

    /// A consumer that signals entry through `onEnter` and completes with the supplied `gate` stage,
    /// for admission-control tests that hold a slot open.
    static SignalConsumer<TelemetryPayload> gated(Runnable onEnter, CompletionStage<DeliveryResult> gate) {
        return new SignalConsumer<>() {
            @Override
            public ConsumerCapabilities capabilities() {
                return ConsumerCapabilities.nonMutating();
            }

            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
                onEnter.run();
                return gate;
            }
        };
    }
}
