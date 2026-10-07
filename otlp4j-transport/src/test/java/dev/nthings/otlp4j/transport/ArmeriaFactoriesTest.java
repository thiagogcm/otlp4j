package dev.nthings.otlp4j.transport;

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
/// into the graph receiver end to end.
@Timeout(60)
@DisplayName("Armeria graph factories")
class ArmeriaFactoriesTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E = ComponentId.parse("otlp");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private @Nullable OtlpReceiver sink;
    private @Nullable GatewayRuntime runtime;
    private @Nullable AutoCloseable client;

    @AfterEach
    void teardown() {
        close(client);
        if (runtime != null) {
            try {
                runtime.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
            } catch (RuntimeException ignored) {
                // keep tearing down
            }
        }
        if (sink != null) {
            try {
                sink.shutdown(Duration.ofSeconds(5)).toCompletableFuture().get(5, TimeUnit.SECONDS);
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

    @DisplayName("a gRPC receiver and exporter sharing an ID forward telemetry through distinct resources")
    @Test
    void grpcGraphRoundTrips() {
        var captured = new AtomicReference<TracesData>();
        sink = OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.record(TRACES, captured))
                .ephemeralPort()
                .build()
                .start();

        var gw = Gateway.builder();
        gw.receiver(R, OtlpReceiverFactory.grpc(),
                OtlpReceiverConfig.defaults(OtlpReceiverFactory.grpc().defaultPort()).withEphemeralPort());
        gw.exporter(E, OtlpExporterFactory.grpc(),
                OtlpExporterConfig.defaults(OtlpExporterFactory.grpc().defaultPort())
                        .toBuilder().endpoint("localhost", sink.port()).build());
        gw.pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)));
        runtime = gw.build();
        runtime.start();

        var graphPort = graphPort();
        var sent = Fixtures.traceData(Fixtures.span("GET /cart", Span.Kind.SERVER));
        var wireClient = OtlpExporter.grpcBuilder().endpoint("localhost", graphPort).build().start();
        client = wireClient;

        var result = wireClient.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(captured.get()).isEqualTo(sent);
    }

    @DisplayName("an HTTP receiver->exporter graph round-trips a wire request")
    @Test
    void httpGraphRoundTrips() {
        var captured = new AtomicReference<TracesData>();
        sink = OtlpReceiver.httpBuilder()
                .route(TRACES, Consumers.record(TRACES, captured))
                .ephemeralPort()
                .build()
                .start();

        var gw = Gateway.builder();
        gw.receiver(R, OtlpReceiverFactory.http(),
                OtlpReceiverConfig.defaults(OtlpReceiverFactory.http().defaultPort()).withEphemeralPort());
        gw.exporter(E, OtlpExporterFactory.http(),
                OtlpExporterConfig.defaults(OtlpExporterFactory.http().defaultPort())
                        .toBuilder().endpoint("localhost", sink.port()).build());
        gw.pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)));
        runtime = gw.build();
        runtime.start();

        var graphPort = graphPort();
        var sent = Fixtures.traceData(Fixtures.span("GET /cart", Span.Kind.SERVER));
        var wireClient = OtlpExporter.httpBuilder().endpoint("localhost", graphPort).build().start();
        client = wireClient;

        var result = wireClient.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(captured.get()).isEqualTo(sent);
    }

    private int graphPort() {
        var receiver = (NetworkBound) runtime.component(ComponentKind.RECEIVER, R).orElseThrow();
        var port = receiver.port();
        assertThat(port).isPositive();
        return port;
    }
}
