package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

/// In-process receiver fake used by graph component tests.
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

    /// Delivers one typed payload into a pipeline head and waits for its acceptance result.
    public <T> DeliveryResult deliver(
            SignalDescriptor<T> signal, ConsumeContext context, Payload<T> payload) {
        var bundle = consumers;
        if (bundle == null) {
            throw new IllegalStateException("receiver not created yet; start the runtime first");
        }
        return bundle.consumer(signal).consume(context, payload).toCompletableFuture().join();
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
