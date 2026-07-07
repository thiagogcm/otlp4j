package dev.nthings.otlp4j.transport.common;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.ModelBatchPayload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// The exporter component adapter: it forwards a payload to the wrapped exporter's per-signal facet,
/// which exports through the shared client and returns a delivery result, and delegates its
/// lifecycle.
@DisplayName("ExporterComponentAdapter")
class ExporterComponentAdapterTest {

    private static final ComponentId ID = ComponentId.parse("otlp/primary");

    /// Test client that captures the exported signal and returns a configurable disposition, always
    /// attributed to the provenance the facet passes in.
    private static final class CapturingClient implements OtlpClient {
        final AtomicReference<SignalDescriptor<?>> exported = new AtomicReference<>();
        final AtomicInteger closes = new AtomicInteger();
        volatile boolean reject;

        @Override
        public <T> CompletionStage<DeliveryResult> export(
                SignalDescriptor<T> signal, T batch, Provenance provenance) {
            exported.set(signal);
            return CompletableFuture.completedFuture(reject
                    ? DeliveryResult.retryable("downstream busy", provenance)
                    : DeliveryResult.accepted(provenance));
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            closes.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static ComponentContext base() {
        return new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
    }

    private static ClientExporter exporter(OtlpClient client) {
        return new ClientExporter(client, ID, "test-exporter", GatewayMetrics.noop());
    }

    private static ModelBatchPayload<TracesData> tracePayload() {
        return new ModelBatchPayload<>(TRACES, new TracesData(List.of()));
    }

    @DisplayName("forwards the payload's batch to the facet and reports acceptance")
    @Test
    void forwardsAndAccepts() {
        var client = new CapturingClient();
        var adapter = new ExporterComponentAdapter<>(ID, TRACES, exporter(client));

        var result = adapter.consume(new TestConsumeContext(), tracePayload()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(result.provenance().componentId()).isEqualTo(ID);
        assertThat(client.exported.get()).isEqualTo(TRACES);
    }

    @DisplayName("carries a retryable delivery rejection back verbatim")
    @Test
    void carriesRejection() {
        var client = new CapturingClient();
        client.reject = true;
        var adapter = new ExporterComponentAdapter<>(ID, TRACES, exporter(client));

        var result = adapter.consume(new TestConsumeContext(), tracePayload()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).disposition()).isEqualTo(Disposition.RETRYABLE);
    }

    @DisplayName("start is a no-op and shutdown/flush delegate to the exporter")
    @Test
    void lifecycleDelegates() {
        var client = new CapturingClient();
        var adapter = new ExporterComponentAdapter<>(ID, TRACES, exporter(client));

        adapter.start(base()).toCompletableFuture().join();
        adapter.forceFlush(new FlushContext(Duration.ofSeconds(1))).toCompletableFuture().join();
        adapter.shutdown(new ShutdownContext(Duration.ofSeconds(1))).toCompletableFuture().join();

        assertThat(client.closes.get()).isEqualTo(1);
        assertThat(adapter.toString()).contains("otlp/primary");
    }

    @DisplayName("create builds the adapter from a transport exporter builder")
    @Test
    void createFromBuilder() {
        var client = new CapturingClient();
        var builder = new TestExporterBuilder(client);
        // The graph passes the terminal signal already cast to the payload-bounded descriptor type;
        // the descriptor constants are keyed by the model batch type, so the test mirrors that cast.
        @SuppressWarnings("unchecked")
        var tracesPayload =
                (dev.nthings.otlp4j.core.signal.SignalDescriptor<
                                dev.nthings.otlp4j.core.signal.TelemetryPayload>)
                        (dev.nthings.otlp4j.core.signal.SignalDescriptor<?>) TRACES;
        var adapter = ExporterComponentAdapter.create(
                builder, OtlpExporterConfig.defaults(4317), ID, tracesPayload, base());

        var result = adapter.consume(new TestConsumeContext(), tracePayload()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(result.provenance().componentId()).isEqualTo(ID);
        assertThat(client.exported.get()).isEqualTo(TRACES);
    }

    /// A concrete exporter builder whose wire client is the capturing test client. It counts how many
    /// times a wire client is built, so a test can assert the shared overload builds one per id.
    private static final class TestExporterBuilder extends AbstractExporterBuilder<TestExporterBuilder> {
        private final OtlpClient client;
        private final AtomicInteger clientBuilds;

        TestExporterBuilder(OtlpClient client) {
            this(client, new AtomicInteger());
        }

        TestExporterBuilder(OtlpClient client, AtomicInteger clientBuilds) {
            super(ClientConfig.builder(), "test-exporter");
            this.client = client;
            this.clientBuilds = clientBuilds;
        }

        @Override
        protected TestExporterBuilder self() {
            return this;
        }

        @Override
        protected OtlpClient newClient(ClientConfig config) {
            clientBuilds.incrementAndGet();
            return client;
        }
    }

    @DisplayName("the shared overload builds one client per id and closes it only after the last facet")
    @Test
    void sharedFacetsShareOneClient() {
        var client = new CapturingClient();
        var clientBuilds = new AtomicInteger();
        var shared = new dev.nthings.otlp4j.core.component.SharedComponents();

        var traces = ExporterComponentAdapter.create(
                new TestExporterBuilder(client, clientBuilds),
                OtlpExporterConfig.defaults(4317), ID, castSignal(TRACES), base(), shared);
        var metrics = ExporterComponentAdapter.create(
                new TestExporterBuilder(client, clientBuilds),
                OtlpExporterConfig.defaults(4317), ID, castSignal(SignalDescriptor.METRICS), base(), shared);

        // One wire client backs both per-signal facets of the single exporter id.
        assertThat(clientBuilds.get()).isEqualTo(1);

        // The shared channel is reference-counted: it closes only when the last facet shuts down.
        traces.shutdown(new ShutdownContext(Duration.ofSeconds(1))).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(0);
        metrics.shutdown(new ShutdownContext(Duration.ofSeconds(1))).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    private static SignalDescriptor<dev.nthings.otlp4j.core.signal.TelemetryPayload> castSignal(
            SignalDescriptor<?> signal) {
        return (SignalDescriptor<dev.nthings.otlp4j.core.signal.TelemetryPayload>) signal;
    }
}
