package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

/// An in-process [ReceiverFactory] fake that boots inside a real [dev.nthings.otlp4j.service.GatewayRuntime]
/// without any transport: it captures the [SignalConsumers] bundle the graph wires into it and exposes
/// [#deliver] so a test drives payloads into the pipeline heads the same way a real receiver would after
/// decoding a request. Its receiver has a no-op lifecycle.
public final class GraphReceiverFactory implements ReceiverFactory<ComponentConfig.None> {

    private volatile @Nullable SignalConsumers consumers;

    @Override
    public ComponentKind kind() {
        return ComponentKind.RECEIVER;
    }

    @Override
    public ComponentConfig.None defaultConfig() {
        return ComponentConfig.NONE;
    }

    @Override
    public String type() {
        return "in";
    }

    @Override
    public Receiver create(CreateContext ctx, ComponentConfig.None config, SignalConsumers consumers) {
        this.consumers = consumers;
        return new NoopReceiver();
    }

    /// Delivers one payload into the pipeline head for `signal`, blocking on the delivery result the
    /// same way a receiver blocks on a decoded request's handling.
    ///
    /// @param signal  the signal to deliver on
    /// @param context the delivery context to carry downstream
    /// @param payload the payload to deliver
    /// @return the delivery result the pipeline produced
    @SuppressWarnings("unchecked")
    public DeliveryResult deliver(SignalDescriptor<?> signal, ConsumeContext context, TelemetryPayload payload) {
        var bundle = consumers;
        if (bundle == null) {
            throw new IllegalStateException("receiver not created yet; start the runtime first");
        }
        var consumer = (SignalConsumer<TelemetryPayload>)
                bundle.consumer((SignalDescriptor<TelemetryPayload>) (SignalDescriptor<?>) signal);
        return consumer.consume(context, payload).toCompletableFuture().join();
    }

    private static final class NoopReceiver implements Receiver {
        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
