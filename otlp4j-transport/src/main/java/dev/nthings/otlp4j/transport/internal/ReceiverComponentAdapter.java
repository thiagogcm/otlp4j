package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.transport.OtlpReceiver;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Presents an owned {@link OtlpReceiver} as a graph receiver component. */
public final class ReceiverComponentAdapter implements Receiver, NetworkBound {

    private final OtlpReceiver receiver;

    public ReceiverComponentAdapter(OtlpReceiver receiver) {
        this.receiver = receiver;
    }

    public static ReceiverComponentAdapter create(
            OtlpReceiver.Builder builder,
            OtlpReceiverConfig config,
            SignalConsumers consumers,
            ComponentContext base,
            ComponentId id) {
        builder.config(config)
                .telemetry(base.metrics())
                .componentId(id)
                .clock(base.clock());
        for (var signal : consumers.signals()) {
            route(builder, consumers, signal);
        }
        return new ReceiverComponentAdapter(builder.build());
    }

    private static <T> void route(
            OtlpReceiver.Builder builder, SignalConsumers consumers, SignalDescriptor<T> signal) {
        builder.route(signal, consumers.consumer(signal));
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        try {
            receiver.start();
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletionStage<FlushReport> flush(FlushContext context) {
        return CompletableFuture.completedFuture(
                FlushReport.empty());
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        return receiver.shutdown(context.deadline());
    }

    @Override
    public int port() {
        return receiver.port();
    }

    @Override
    public String toString() {
        return "OtlpReceiver(" + receiver.port() + ")";
    }
}
