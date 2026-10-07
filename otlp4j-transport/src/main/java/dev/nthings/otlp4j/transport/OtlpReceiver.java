package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.internal.ArmeriaOtlpServer;
import dev.nthings.otlp4j.transport.internal.Protocol;
import dev.nthings.otlp4j.transport.internal.ReceiverTap;
import dev.nthings.otlp4j.transport.internal.SignalSource;
import java.time.Clock;
import java.time.Duration;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/** An owned OTLP receiver runtime backed by one Armeria server. */
public final class OtlpReceiver implements AutoCloseable {

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private enum State { NEW, STARTED, STOPPING, STOPPED }

    private final Map<SignalDescriptor<?>, SignalSource<?>> sources = new LinkedHashMap<>();
    private final ReceiverTap tap;
    private final ArmeriaOtlpServer server;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    private CompletionStage<Void> shutdownStage;

    private OtlpReceiver(
            ComponentId id,
            GatewayMetrics telemetry,
            ArmeriaOtlpServer server,
            Map<SignalDescriptor<?>, ? extends SignalConsumer<?>> routes) {
        this.tap = new ReceiverTap(telemetry);
        for (var signal : SignalDescriptor.values()) {
            sources.put(signal, new SignalSource<>(signal, id));
        }
        routes.forEach((signal, consumer) -> attachUnchecked(signal, consumer));
        this.server = server;
    }

    public static Builder grpcBuilder() {
        return new Builder(EnumSet.of(Protocol.GRPC), OtlpReceiverConfig.builder(Ports.GRPC));
    }

    public static Builder httpBuilder() {
        return new Builder(EnumSet.of(Protocol.HTTP), OtlpReceiverConfig.builder(Ports.HTTP));
    }

    public static Builder builder() {
        return new Builder(
                EnumSet.of(Protocol.GRPC, Protocol.HTTP), OtlpReceiverConfig.builder(Ports.GRPC));
    }

    /** Starts the socket synchronously. Calls after a successful start are idempotent. */
    public synchronized OtlpReceiver start() {
        var current = state.get();
        if (current == State.STARTED) {
            return this;
        }
        if (current != State.NEW) {
            throw new IllegalStateException("receiver is already stopped");
        }
        try {
            server.start();
        } catch (Exception failure) {
            tap.close();
            throw failure instanceof RuntimeException runtime
                    ? runtime
                    : new IllegalStateException("failed to start receiver", failure);
        }
        state.set(State.STARTED);
        return this;
    }

    public boolean isStarted() {
        return state.get() == State.STARTED;
    }

    public int port() {
        return server.port();
    }

    public synchronized CompletionStage<Void> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        if (shutdownStage != null) {
            return shutdownStage;
        }
        if (state.get() == State.NEW) {
            state.set(State.STOPPED);
            tap.close();
            shutdownStage = CompletableFuture.completedStage(null);
            return shutdownStage;
        }
        state.set(State.STOPPING);
        var result = new CompletableFuture<Void>();
        shutdownStage = result.minimalCompletionStage();
        server.shutdown(timeout).whenComplete((ignored, failure) -> {
            tap.close();
            state.set(State.STOPPED);
            if (failure == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(failure);
            }
        });
        return shutdownStage;
    }

    @Override
    public void close() {
        shutdown(DEFAULT_CLOSE_TIMEOUT).toCompletableFuture().join();
    }

    private CompletionStage<DeliveryResult> dispatch(ConsumeContext context, Payload<?> payload) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(payload, "payload");
        tap.publish(payload);
        return dispatchTyped(context, payload);
    }

    private <T> CompletionStage<DeliveryResult> dispatchTyped(
            ConsumeContext context, Payload<T> payload) {
        return sourceFor(payload.signal()).dispatch(context, payload);
    }

    private <T> SignalSource<T> sourceFor(SignalDescriptor<T> signal) {
        @SuppressWarnings("unchecked")
        var source = (SignalSource<T>) sources.get(signal);
        return source;
    }

    private <T> void attach(SignalDescriptor<T> signal, SignalConsumer<T> consumer) {
        sourceFor(signal).attach(consumer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void attachUnchecked(SignalDescriptor<?> signal, SignalConsumer<?> consumer) {
        attach((SignalDescriptor) signal, (SignalConsumer) consumer);
    }

    /** Builder for an owned receiver. */
    public static final class Builder {

        private final Set<Protocol> protocols;
        private OtlpReceiverConfig.Builder config;
        private final Map<SignalDescriptor<?>, SignalConsumer<?>> routes = new LinkedHashMap<>();
        private ComponentId id = ComponentId.parse("otlp");
        private Clock clock = Clock.systemUTC();
        private GatewayMetrics telemetry = GatewayMetrics.noop();

        private Builder(Set<Protocol> protocols, OtlpReceiverConfig.Builder config) {
            this.protocols = Set.copyOf(protocols);
            this.config = config;
        }

        public Builder config(OtlpReceiverConfig config) {
            this.config = Objects.requireNonNull(config, "config").toBuilder();
            return this;
        }

        public Builder endpoint(String bindHost, int port) {
            config.bindHost(bindHost).port(port);
            return this;
        }

        public Builder port(int port) {
            config.port(port);
            return this;
        }

        public Builder ephemeralPort() {
            return port(0);
        }

        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        public Builder maxInboundMessageSizeBytes(int bytes) {
            config.maxInboundMessageSizeBytes(bytes);
            return this;
        }

        public Builder maxClientTimeout(Duration timeout) {
            config.maxClientTimeout(timeout);
            return this;
        }

        public Builder maxInFlightRequests(int max) {
            config.maxInFlightRequests(max);
            return this;
        }

        public Builder maxInFlightBytes(long bytes) {
            config.maxInFlightBytes(bytes);
            return this;
        }

        public Builder throttleRetryAfter(Duration retryAfter) {
            config.throttleRetryAfter(retryAfter);
            return this;
        }

        public Builder clientAuth(ClientAuth clientAuth) {
            config.clientAuth(clientAuth);
            return this;
        }

        public <T> Builder route(SignalDescriptor<T> signal, SignalConsumer<T> consumer) {
            routes.put(
                    Objects.requireNonNull(signal, "signal"),
                    Objects.requireNonNull(consumer, "consumer"));
            return this;
        }

        public Builder componentId(ComponentId id) {
            this.id = Objects.requireNonNull(id, "id");
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder telemetry(GatewayMetrics telemetry) {
            this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        public OtlpReceiver build() {
            var built = config.build();
            // Constructing the Armeria server does not bind or allocate a client/server event loop.
            OtlpReceiver[] holder = new OtlpReceiver[1];
            var server = new ArmeriaOtlpServer(
                    built,
                    (context, payload) -> holder[0].dispatch(context, payload),
                    telemetry,
                    protocols,
                    clock);
            holder[0] = new OtlpReceiver(id, telemetry, server, routes);
            return holder[0];
        }
    }
}
