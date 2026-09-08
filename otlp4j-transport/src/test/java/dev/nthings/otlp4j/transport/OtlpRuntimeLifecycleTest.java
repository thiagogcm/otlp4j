package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
@DisplayName("owned OTLP runtimes")
class OtlpRuntimeLifecycleTest {

    private static final Duration SHORT = Duration.ofSeconds(2);
    private static final ComponentContext COMPONENT_CONTEXT =
            new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
    private static final Payload<TracesData> EMPTY_TRACES =
            Payload.of(TRACES, new TracesData(List.of()));

    @Test
    @DisplayName("an exporter is inert until started and rejects work after shutdown")
    void exporterHasExplicitLifecycle() {
        var exporter = OtlpExporter.httpBuilder()
                .endpoint("127.0.0.1", 1)
                .build();
        assertThat(exporter.isStarted()).isFalse();
        assertThat(exporter.flush(SHORT).toCompletableFuture().join()).isEqualTo(
                FlushReport.empty());

        exporter.shutdown(SHORT).toCompletableFuture().join();
        assertThat(exporter.isStarted()).isFalse();
        assertThatThrownBy(exporter::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already stopped");
        assertThat(exporter.shutdown(SHORT).toCompletableFuture().join()).isNull();
        assertThatThrownBy(() -> exporter.export(TRACES, EMPTY_TRACES.data()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not been started");
    }

    @Test
    @DisplayName("an exporter starts idempotently, bounds context, flushes, and shuts down")
    void exporterLifecycleAndContext() {
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("127.0.0.1", 1)
                .build();
        assertThat(exporter.start()).isSameAs(exporter);
        assertThat(exporter.start()).isSameAs(exporter);
        assertThat(exporter.isStarted()).isTrue();
        assertThat(exporter.flush(SHORT).toCompletableFuture().join()).isEqualTo(
                FlushReport.empty());

        var expired = ConsumeContext.of(
                ClientMetadata.empty(), new Deadline(Instant.now().minusSeconds(1)));
        assertThatThrownBy(() -> exporter.export(expired, EMPTY_TRACES).toCompletableFuture().join())
                .hasCauseInstanceOf(CancellationException.class);

        exporter.shutdown(SHORT).toCompletableFuture().join();
        assertThat(exporter.isStarted()).isFalse();
        assertThat(exporter.shutdown(SHORT).toCompletableFuture().join()).isNull();
    }

    @Test
    @DisplayName("shared graph facets retain one exporter until the final shutdown")
    void sharedFacetsUseReferenceCounting() {
        var id = ComponentId.parse("otlp/shared");
        var create = new CreateContext(id, COMPONENT_CONTEXT);
        var config = OtlpExporterConfig.defaults(4317)
                .toBuilder()
                .endpoint("127.0.0.1", 1)
                .build();
        var factory = OtlpExporterFactory.grpc();
        Exporter<?> traces = factory.create(create, config, TRACES);
        Exporter<?> metrics = factory.create(create, config, METRICS);
        var owner = create.sharedComponents().computeIfAbsent(id, () -> {
            throw new AssertionError("factory did not register its shared exporter");
        });

        assertThat(traces.toString()).contains("traces");
        assertThat(metrics.toString()).contains("metrics");
        traces.start(COMPONENT_CONTEXT).toCompletableFuture().join();
        assertThat(owner).isInstanceOf(OtlpExporter.class);
        var exporter = (OtlpExporter) owner;
        assertThat(exporter.isStarted()).isTrue();
        traces.flush(new FlushContext(SHORT)).toCompletableFuture().join();

        traces.shutdown(new ShutdownContext(SHORT)).toCompletableFuture().join();
        assertThat(exporter.isStarted()).isTrue();
        metrics.shutdown(new ShutdownContext(SHORT)).toCompletableFuture().join();
        assertThat(exporter.isStarted()).isFalse();
    }

    @Test
    @DisplayName("a receiver binds one port through its owned lifecycle")
    void receiverLifecycle() {
        var receiver = OtlpReceiver.httpBuilder().ephemeralPort().build();
        assertThat(receiver.isStarted()).isFalse();
        assertThat(receiver.port()).isZero();
        assertThat(receiver.start()).isSameAs(receiver);
        assertThat(receiver.start()).isSameAs(receiver);
        assertThat(receiver.isStarted()).isTrue();
        assertThat(receiver.port()).isPositive();

        var shutdown = receiver.shutdown(Duration.ofSeconds(5));
        shutdown.toCompletableFuture().join();
        assertThat(receiver.isStarted()).isFalse();
        assertThat(receiver.shutdown(Duration.ofSeconds(1)) == shutdown).isTrue();
    }

    @Test
    @DisplayName("runtime lifecycle methods reject negative deadlines")
    void negativeDeadlinesRejected() {
        var exporter = OtlpExporter.httpBuilder().build();
        assertThatThrownBy(() -> exporter.flush(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> exporter.shutdown(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        var receiver = OtlpReceiver.httpBuilder().build();
        assertThatThrownBy(() -> receiver.shutdown(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        receiver.shutdown(Duration.ZERO).toCompletableFuture().join();
        assertThatThrownBy(receiver::start).isInstanceOf(IllegalStateException.class);
        exporter.close();
    }
}
