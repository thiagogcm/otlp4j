package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Test-only typed consumer factories. */
final class Consumers {

    static final Provenance PROV =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    private Consumers() {}

    static <T> SignalConsumer<T> of(Function<Payload<T>, DeliveryResult> fn) {
        return (context, data) -> CompletableFuture.completedFuture(fn.apply(data));
    }

    static <T> SignalConsumer<T> accepting() {
        return of(p -> DeliveryResult.accepted(PROV));
    }

    static <T> SignalConsumer<T> record(
            SignalDescriptor<T> signal, AtomicReference<? super T> sink) {
        return of(p -> {
            sink.set(p.data());
            return DeliveryResult.accepted(PROV);
        });
    }

    static <T> SignalConsumer<T> partial(long rejected, String message) {
        return of(p -> DeliveryResult.partial(rejected, message, PROV));
    }

    static <T> SignalConsumer<T> throttled(Duration retryAfter, String message) {
        return of(p -> DeliveryResult.throttled(retryAfter, message, PROV));
    }

    static <T> SignalConsumer<T> permanent(String message) {
        return of(p -> DeliveryResult.permanent(message, PROV));
    }

    static <T> SignalConsumer<T> retryableWithCause(String message, Throwable cause) {
        return of(p -> DeliveryResult.retryable(message, cause, PROV));
    }

    static <T> SignalConsumer<T> throwing(RuntimeException error) {
        return of(p -> {
            throw error;
        });
    }

    static <T> SignalConsumer<T> gated(
            Runnable onEnter, CompletionStage<DeliveryResult> gate) {
        return (context, data) -> {
            onEnter.run();
            return gate;
        };
    }
}
