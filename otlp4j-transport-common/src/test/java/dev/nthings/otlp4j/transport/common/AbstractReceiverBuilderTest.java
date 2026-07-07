package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.ModelBatchPayload;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.jspecify.annotations.Nullable;

/// Exercises the shared receiver-builder base through a test subtype whose `newServer` captures
/// the built [ServerConfig] and the receiver's [Dispatchers] - no reflection into builder
/// internals.
@DisplayName("AbstractReceiverBuilder")
class AbstractReceiverBuilderTest {

    /// Inert transport server; never binds a port.
    private static final class StubServer implements OtlpServer {

        @Override
        public void start() {}

        @Override
        public int port() {
            return 0;
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

    /// Minimal concrete builder: records the config and dispatchers handed to `newServer`.
    private static final class TestBuilder extends AbstractReceiverBuilder<TestBuilder> {

        @Nullable ServerConfig captured;
        @Nullable Dispatchers dispatchers;
        @Nullable GatewayMetrics capturedTelemetry;

        TestBuilder() {
            super(ServerConfig.builder(), "OTLP/test");
        }

        @Override
        protected TestBuilder self() {
            return this;
        }

        @Override
        protected OtlpServer newServer(
                ServerConfig config, Dispatchers dispatchers, GatewayMetrics telemetry) {
            this.captured = config;
            this.dispatchers = dispatchers;
            this.capturedTelemetry = telemetry;
            return new StubServer();
        }
    }

    @DisplayName("every shared setter lands on the built ServerConfig")
    @Test
    void sharedSettersLandOnConfig() {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var builder = new TestBuilder();
        builder.setEndpoint("127.0.0.1", 4317)
                .setTls(Tls.disabled())
                .setMaxInboundMessageSizeBytes(1024)
                .setHandshakeTimeout(Duration.ofSeconds(5))
                .setServerExecutor(executor)
                .build();
        var c = builder.captured;
        assertThat(c).isNotNull();
        assertThat(c.bindHost()).isEqualTo("127.0.0.1");
        assertThat(c.port()).isEqualTo(4317);
        assertThat(c.tls()).isEqualTo(Tls.disabled());
        assertThat(c.maxInboundMessageSizeBytes()).isEqualTo(1024);
        assertThat(c.handshakeTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(c.serverExecutor()).isSameAs(executor);
        executor.shutdown();
    }

    @DisplayName("telemetry defaults to noop and the explicit registry reaches newServer")
    @Test
    void telemetryKnobThreadsToNewServer() {
        var defaulted = new TestBuilder();
        defaulted.build();
        assertThat(defaulted.capturedTelemetry).isSameAs(GatewayMetrics.noop());

        var registry = new dev.nthings.otlp4j.telemetry.LoggingGatewayMetrics();
        var configured = new TestBuilder();
        configured.telemetry(registry).build();
        assertThat(configured.capturedTelemetry).isSameAs(registry);
    }

    @DisplayName("ephemeralPort() binds port 0 and setConfig replaces prior state")
    @Test
    void ephemeralPortAndSetConfig() {
        var ephemeral = new TestBuilder();
        ephemeral.setPort(4317).ephemeralPort().build();
        assertThat(ephemeral.captured).isNotNull();
        assertThat(ephemeral.captured.port()).isZero();

        var replacement = ServerConfig.builder().setBindHost("0.0.0.0").setPort(9999).build();
        var replaced = new TestBuilder();
        replaced.setPort(1).setConfig(replacement).build();
        assertThat(replaced.captured).isNotNull();
        assertThat(replaced.captured.bindHost()).isEqualTo("0.0.0.0");
        assertThat(replaced.captured.port()).isEqualTo(9999);
    }

    @DisplayName("a routed consumer is reached: dispatching a payload reaches it")
    @Test
    void consumerRoutingDispatches() {
        var seen = new AtomicReference<TelemetryPayload>();
        var builder = new TestBuilder();
        var receiver = builder.route(TRACES, recording(seen)).build();

        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var payload = new ModelBatchPayload<>(TRACES, batch);
        assertThat(builder.dispatchers).isNotNull();
        var result = builder.dispatchers.dispatch(payload).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get()).isSameAs(payload);
        receiver.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
    }

    @DisplayName("an unrouted signal rejects retryably until a consumer is routed")
    @Test
    void unroutedSignalRejectsRetryably() {
        var builder = new TestBuilder();
        var receiver = builder.build();

        assertThat(builder.dispatchers).isNotNull();
        var result = builder.dispatchers
                .dispatch(new ModelBatchPayload<>(
                        TRACES, Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))))
                .toCompletableFuture()
                .join();

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).disposition()).isEqualTo(Disposition.RETRYABLE);
        receiver.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
    }

    private static SignalConsumer<TelemetryPayload> recording(AtomicReference<TelemetryPayload> seen) {
        var provenance = new Provenance(
                dev.nthings.otlp4j.core.component.ComponentId.parse("sink"), ComponentKind.EXPORTER, TRACES);
        return new SignalConsumer<>() {
            @Override
            public ConsumerCapabilities capabilities() {
                return ConsumerCapabilities.nonMutating();
            }

            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
                seen.set(data);
                return DeliveryResult.acceptedStage(provenance);
            }
        };
    }
}
