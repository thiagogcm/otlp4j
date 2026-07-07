package dev.nthings.otlp4j.transport.grpc;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.StabilityLevel;
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

/// The OTLP/gRPC receiver and exporter factories, exercised through a real service graph runtime: a
/// graph of `receiver -> exporter` booted from a [dev.nthings.otlp4j.service.config.GatewayConfig],
/// with the exporter forwarding to a plain recording receiver, and a wire request driven into the
/// graph receiver end to end.
@Timeout(30)
@DisplayName("OTLP/gRPC graph factories")
class OtlpGrpcFactoriesTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E = ComponentId.parse("otlpexp");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private @org.jspecify.annotations.Nullable ServerReceiver sink;
    private @org.jspecify.annotations.Nullable GatewayRuntime runtime;
    private @org.jspecify.annotations.Nullable AutoCloseable client;

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

    private static void close(@org.jspecify.annotations.Nullable AutoCloseable c) {
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
        var recv = new OtlpGrpcReceiverFactory();
        var exp = new OtlpGrpcExporterFactory();
        assertThat(recv.kind()).isEqualTo(ComponentKind.RECEIVER);
        assertThat(exp.kind()).isEqualTo(ComponentKind.EXPORTER);
        assertThat(recv.stability()).isEqualTo(StabilityLevel.BETA);
        assertThat(exp.stability()).isEqualTo(StabilityLevel.BETA);
        assertThat(recv.type()).isEqualTo("otlp");
        assertThat(exp.type()).isEqualTo("otlp");
        assertThat(recv.defaultConfig().port()).isEqualTo(OtlpGrpcReceiverFactory.DEFAULT_PORT);
        assertThat(exp.defaultConfig().port()).isEqualTo(OtlpGrpcExporterFactory.DEFAULT_PORT);
    }

    @DisplayName("a receiver->exporter graph round-trips a wire request to a downstream endpoint")
    @Test
    void receiverToExporterGraphRoundTrips() {
        var captured = new AtomicReference<TracesData>();
        sink = OtlpGrpcReceiver.builder()
                .route(TRACES, Consumers.record(TRACES, captured))
                .ephemeralPort()
                .build()
                .start();
        var sinkPort = sink.port();

        var receiverConfig = OtlpReceiverConfig.defaults(OtlpGrpcReceiverFactory.DEFAULT_PORT).withEphemeralPort();
        var exporterConfig = OtlpExporterConfig.defaults(OtlpGrpcExporterFactory.DEFAULT_PORT)
                .toBuilder()
                .setEndpoint("localhost", sinkPort)
                .build();

        runtime = Gateway.builder()
                .receiver(R, new OtlpGrpcReceiverFactory(), receiverConfig)
                .exporter(E, new OtlpGrpcExporterFactory(), exporterConfig)
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .build();
        runtime.start();

        var graphPort = runtime.component(R)
                .filter(NetworkBound.class::isInstance)
                .map(NetworkBound.class::cast)
                .map(NetworkBound::port)
                .orElseThrow();
        assertThat(graphPort).isPositive();

        var sent = Fixtures.traceData(Fixtures.span("GET /cart", Span.Kind.SERVER));
        var wireClient = OtlpGrpcExporter.to("localhost", graphPort);
        client = wireClient;

        var result = wireClient.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(captured.get()).isEqualTo(sent);
    }
}
