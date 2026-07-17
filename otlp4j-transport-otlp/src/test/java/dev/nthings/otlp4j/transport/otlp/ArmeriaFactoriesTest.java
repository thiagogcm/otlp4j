package dev.nthings.otlp4j.transport.otlp;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.transport.common.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.jspecify.annotations.Nullable;

/// The Armeria receiver and exporter factories, exercised through a real service graph runtime: a
/// `receiver -> exporter` graph forwarding to a plain recording receiver, with a wire request driven
/// into the graph receiver end to end. Mirrors the legacy transport factory tests.
@Timeout(60)
@DisplayName("Armeria graph factories")
class ArmeriaFactoriesTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E = ComponentId.parse("otlpexp");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private @Nullable ServerReceiver sink;
    private @Nullable GatewayRuntime runtime;
    private @Nullable AutoCloseable client;

    @AfterEach
    void teardown() {
        close(client);
        if (runtime != null) {
            try {
                runtime.shutdown(Duration.ofSeconds(5));
            } catch (RuntimeException ignored) {
                // keep tearing down
            }
        }
        if (sink != null) {
            try {
                sink.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    private static void close(@Nullable AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    @DisplayName("factory metadata is declared honestly")
    @Test
    void factoryMetadata() {
        var grpcRecv = new OtlpArmeriaGrpcReceiverFactory();
        var grpcExp = new OtlpArmeriaGrpcExporterFactory();
        var httpRecv = new OtlpArmeriaHttpReceiverFactory();
        var httpExp = new OtlpArmeriaHttpExporterFactory();

        assertThat(grpcRecv.kind()).isEqualTo(ComponentKind.RECEIVER);
        assertThat(httpRecv.kind()).isEqualTo(ComponentKind.RECEIVER);
        assertThat(grpcExp.kind()).isEqualTo(ComponentKind.EXPORTER);
        assertThat(httpExp.kind()).isEqualTo(ComponentKind.EXPORTER);

        assertThat(grpcRecv.type()).isEqualTo("otlp");
        assertThat(grpcExp.type()).isEqualTo("otlp");
        assertThat(httpRecv.type()).isEqualTo("otlphttp");
        assertThat(httpExp.type()).isEqualTo("otlphttp");

        assertThat(grpcRecv.defaultConfig().port()).isEqualTo(OtlpArmeriaGrpcReceiverFactory.DEFAULT_PORT);
        assertThat(grpcExp.defaultConfig().port()).isEqualTo(OtlpArmeriaGrpcExporterFactory.DEFAULT_PORT);
        assertThat(httpRecv.defaultConfig().port()).isEqualTo(OtlpArmeriaHttpReceiverFactory.DEFAULT_PORT);
        assertThat(httpExp.defaultConfig().port()).isEqualTo(OtlpArmeriaHttpExporterFactory.DEFAULT_PORT);
    }

    @DisplayName("a gRPC receiver->exporter graph round-trips a wire request")
    @Test
    void grpcGraphRoundTrips() {
        var captured = new AtomicReference<TracesData>();
        sink = OtlpArmeriaReceiver.grpcBuilder()
                .route(TRACES, Consumers.record(TRACES, captured))
                .ephemeralPort()
                .build()
                .start();

        runtime = Gateway.builder()
                .receiver(R, new OtlpArmeriaGrpcReceiverFactory(),
                        OtlpReceiverConfig.defaults(OtlpArmeriaGrpcReceiverFactory.DEFAULT_PORT).withEphemeralPort())
                .exporter(E, new OtlpArmeriaGrpcExporterFactory(),
                        OtlpExporterConfig.defaults(OtlpArmeriaGrpcExporterFactory.DEFAULT_PORT)
                                .toBuilder().setEndpoint("localhost", sink.port()).build())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .build();
        runtime.start();

        var graphPort = graphPort();
        var sent = Fixtures.traceData(Fixtures.span("GET /cart", Span.Kind.SERVER));
        var wireClient = OtlpArmeriaExporter.grpcBuilder().setEndpoint("localhost", graphPort).build();
        client = wireClient;

        var result = wireClient.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(captured.get()).isEqualTo(sent);
    }

    @DisplayName("an HTTP receiver->exporter graph round-trips a wire request")
    @Test
    void httpGraphRoundTrips() {
        var captured = new AtomicReference<TracesData>();
        sink = OtlpArmeriaReceiver.httpBuilder()
                .route(TRACES, Consumers.record(TRACES, captured))
                .ephemeralPort()
                .build()
                .start();

        runtime = Gateway.builder()
                .receiver(R, new OtlpArmeriaHttpReceiverFactory(),
                        OtlpReceiverConfig.defaults(OtlpArmeriaHttpReceiverFactory.DEFAULT_PORT).withEphemeralPort())
                .exporter(E, new OtlpArmeriaHttpExporterFactory(),
                        OtlpExporterConfig.defaults(OtlpArmeriaHttpExporterFactory.DEFAULT_PORT)
                                .toBuilder().setEndpoint("localhost", sink.port()).build())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .build();
        runtime.start();

        var graphPort = graphPort();
        var sent = Fixtures.traceData(Fixtures.span("GET /cart", Span.Kind.SERVER));
        var wireClient = OtlpArmeriaExporter.httpBuilder().setEndpoint("localhost", graphPort).build();
        client = wireClient;

        var result = wireClient.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(captured.get()).isEqualTo(sent);
    }

    private int graphPort() {
        var port = runtime.component(R)
                .filter(NetworkBound.class::isInstance)
                .map(NetworkBound.class::cast)
                .map(NetworkBound::port)
                .orElseThrow();
        assertThat(port).isPositive();
        return port;
    }
}
