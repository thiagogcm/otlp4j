package dev.nthings.otlp4j.transport.common;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.codec.DomainRequestPayload;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// The receiver component adapter: it bridges the transport receiver's per-signal sinks onto the
/// graph consumers, delegates the bound port, and maps the lifecycle onto the legacy receiver.
@DisplayName("ReceiverComponentAdapter")
class ReceiverComponentAdapterTest {

    private static final int STUB_PORT = 54321;

    private static final ComponentId RECEIVER = ComponentId.parse("otlp/in");

    private static ComponentContext base() {
        return new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
    }

    @DisplayName("routes a dispatched payload to the graph consumer and returns its result")
    @Test
    void routesDispatchToConsumer() {
        var captured = new AtomicReference<TelemetryPayload>();
        var consumers = new SingleConsumer(captured);
        var builder = new TestReceiverBuilder(false);

        var adapter = ReceiverComponentAdapter.create(
                builder, OtlpReceiverConfig.defaults(0).withEphemeralPort(), consumers, base(), RECEIVER);
        adapter.start(base()).toCompletableFuture().join();

        var dispatchers = builder.dispatchers;
        assertThat(dispatchers).isNotNull();
        var result = dispatchers
                .dispatch(Payloads.domain(TRACES, new TracesData(List.of())))
                .toCompletableFuture()
                .join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(captured.get()).isInstanceOf(DomainRequestPayload.class);
        assertThat(captured.get().signal()).isEqualTo(TRACES);
        assertThat(adapter.port()).isEqualTo(STUB_PORT);
    }

    @DisplayName("only signals present in the bundle are routed")
    @Test
    void onlyBundledSignalsRouted() {
        var consumers = new SingleConsumer(new AtomicReference<>());
        var builder = new TestReceiverBuilder(false);
        ReceiverComponentAdapter.create(builder, OtlpReceiverConfig.defaults(0), consumers, base(), RECEIVER);

        // A metrics dispatch has no routed consumer, so the source reports no consumer rather than
        // routing into the (traces-only) bundle.
        var result = builder.dispatchers
                .dispatch(Payloads.domain(
                        dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS,
                        new dev.nthings.otlp4j.model.MetricsData(List.of())))
                .toCompletableFuture()
                .join();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
    }

    @DisplayName("start surfaces a bind failure as a failed stage")
    @Test
    void startSurfacesBindFailure() {
        var consumers = new SingleConsumer(new AtomicReference<>());
        var builder = new TestReceiverBuilder(true);
        var adapter = ReceiverComponentAdapter.create(
                builder, OtlpReceiverConfig.defaults(0), consumers, base(), RECEIVER);

        // The bind runs off the caller's thread now, so wait for the stage to settle before asserting
        // it failed rather than reading isCompletedExceptionally() before the bind has run.
        var start = adapter.start(base()).toCompletableFuture();
        assertThat(start).failsWithin(Duration.ofSeconds(2));
        assertThat(start.isCompletedExceptionally()).isTrue();

        adapter.shutdown(new ShutdownContext(Duration.ofSeconds(1))).toCompletableFuture().join();
    }

    /// A bundle of exactly one traces consumer that records the payload it receives.
    private static final class SingleConsumer implements SignalConsumers {
        private final AtomicReference<TelemetryPayload> captured;

        SingleConsumer(AtomicReference<TelemetryPayload> captured) {
            this.captured = captured;
        }

        @Override
        public Set<SignalDescriptor<?>> signals() {
            return Set.of(TRACES);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends TelemetryPayload> SignalConsumer<T> consumer(SignalDescriptor<T> signal) {
            return (SignalConsumer<T>) new Recording(captured);
        }
    }

    private static final class Recording implements SignalConsumer<TelemetryPayload> {
        private final AtomicReference<TelemetryPayload> captured;

        Recording(AtomicReference<TelemetryPayload> captured) {
            this.captured = captured;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
            // Touch the synthesized context so its metadata, deadline, and cancellation surface are
            // exercised: the receiver adapter hands every consumer a ReceiverConsumeContext.
            context.metadata();
            context.deadline();
            context.isCancelled();
            context.onCancel(() -> {});
            captured.set(data);
            return DeliveryResult.acceptedStage(
                    new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, TRACES));
        }
    }

    /// A concrete receiver builder whose transport server captures the dispatchers and can fail its
    /// bind.
    private static final class TestReceiverBuilder extends AbstractReceiverBuilder<TestReceiverBuilder> {
        private final boolean failBind;
        @Nullable Dispatchers dispatchers;

        TestReceiverBuilder(boolean failBind) {
            super(ServerConfig.builder(), "OTLP/test");
            this.failBind = failBind;
        }

        @Override
        protected TestReceiverBuilder self() {
            return this;
        }

        @Override
        protected OtlpServer newServer(ServerConfig config, Dispatchers dispatchers, GatewayMetrics telemetry) {
            this.dispatchers = dispatchers;
            return new StubServer(failBind);
        }
    }

    private static final class StubServer implements OtlpServer {
        private final boolean failBind;

        StubServer(boolean failBind) {
            this.failBind = failBind;
        }

        @Override
        public void start() throws IOException {
            if (failBind) {
                throw new IOException("bind refused on purpose");
            }
        }

        @Override
        public int port() {
            return STUB_PORT;
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdownNow() {
            return CompletableFuture.completedFuture(null);
        }
    }
}
