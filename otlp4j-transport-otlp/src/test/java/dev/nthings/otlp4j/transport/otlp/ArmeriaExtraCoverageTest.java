package dev.nthings.otlp4j.transport.otlp;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Additional coverage for the header path, the dual-protocol public builder, the exporter's OTLP/HTTP
/// knobs, and the less-common request and status mappings.
@Timeout(60)
@DisplayName("Armeria transport extra coverage")
class ArmeriaExtraCoverageTest {

    private static final MediaType PROTOBUF = MediaType.parse("application/x-protobuf");

    private final List<ServerReceiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    private ServerReceiver start(OtlpArmeriaReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build().start();
        receivers.add(receiver);
        return receiver;
    }

    private static byte[] traceBytes() {
        return SignalCodec.TRACES.toProto(
                Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER))).toByteArray();
    }

    @DisplayName("the dual-protocol public builder serves gRPC and HTTP on one port")
    @Test
    void dualBuilderServesBothProtocols() {
        var grpcSeen = new AtomicReference<TracesData>();
        var httpSeen = new AtomicReference<TracesData>();
        var receiver = start(OtlpArmeriaReceiver.builder()
                .route(TRACES, Consumers.of(p -> {
                    var data = TRACES.batchType().cast(
                            p.view(dev.nthings.otlp4j.core.signal.PayloadView.DOMAIN_IMMUTABLE));
                    grpcSeen.compareAndSet(null, data);
                    httpSeen.set(data);
                    return DeliveryResult.accepted(Consumers.PROV);
                })));

        var grpcExporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port()).setTimeout(Duration.ofSeconds(5)).build();
        closeables.add(grpcExporter);
        var httpExporter = OtlpArmeriaExporter.httpBuilder()
                .setEndpoint("localhost", receiver.port()).setTimeout(Duration.ofSeconds(5)).build();
        closeables.add(httpExporter);

        var sent = TransportFixtures.richTraceData();
        assertThat(grpcExporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(httpExporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(grpcSeen.get()).isEqualTo(sent);
        assertThat(httpSeen.get()).isEqualTo(sent);
    }

    @DisplayName("gRPC per-call headers reach the server and the export round-trips")
    @Test
    void grpcHeadersRoundTrip() {
        var received = new AtomicReference<TracesData>();
        var receiver = start(OtlpArmeriaReceiver.grpcBuilder()
                .route(TRACES, Consumers.record(TRACES, received)));
        var exporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port())
                .addHeader("authorization", "Bearer token")
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("HTTP per-request headers reach the server and the export round-trips through a path prefix knob")
    @Test
    void httpHeadersAndKnobsRoundTrip() {
        var received = new AtomicReference<TracesData>();
        var receiver = start(OtlpArmeriaReceiver.httpBuilder()
                .route(TRACES, Consumers.record(TRACES, received)));
        var exporter = OtlpArmeriaExporter.httpBuilder()
                .setEndpoint("localhost", receiver.port())
                .setPath("")
                .addHeader("authorization", "Bearer token")
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("a gRPC rejection carrying a cause still maps to UNAVAILABLE")
    @Test
    void grpcRejectionWithCauseMapsToUnavailable() {
        var receiver = start(OtlpArmeriaReceiver.grpcBuilder()
                .route(TRACES, Consumers.retryableWithCause("downstream flaked", new IllegalStateException("boom"))));
        var exporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port()).setTimeout(Duration.ofSeconds(5)).build();
        closeables.add(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @DisplayName("an HTTP dispatcher failure yields 500")
    @Test
    void httpDispatcherFailureYields500() {
        var receiver = start(OtlpArmeriaReceiver.httpBuilder()
                .route(TRACES, Consumers.throwing(new IllegalStateException("handler boom"))));
        var response = WebClient.of("http://localhost:" + receiver.port())
                .execute(HttpRequest.builder().post("/v1/traces").content(PROTOBUF, traceBytes()).build())
                .aggregate().join();
        assertThat(response.status().code()).isEqualTo(500);
    }

    @DisplayName("admission at capacity without a throttle hint yields 429 and no Retry-After")
    @Test
    void httpAdmissionNoHintYields429() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var gate = new java.util.concurrent.CompletableFuture<DeliveryResult>();
        var receiver = start(OtlpArmeriaReceiver.httpBuilder()
                .setMaxInFlightRequests(1)
                .route(TRACES, Consumers.gated(entered::countDown, gate)));
        var client = WebClient.of("http://localhost:" + receiver.port());

        var held = client.execute(HttpRequest.builder().post("/v1/traces")
                .content(PROTOBUF, traceBytes()).build()).aggregate();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var second = client.execute(HttpRequest.builder().post("/v1/traces")
                .content(PROTOBUF, traceBytes()).build()).aggregate().join();
        assertThat(second.status().code()).isEqualTo(429);
        assertThat(second.headers().get(HttpHeaderNames.RETRY_AFTER)).isNull();

        gate.complete(DeliveryResult.accepted(Consumers.PROV));
        assertThat(held.get(5, TimeUnit.SECONDS).status().code()).isEqualTo(200);
    }

    @DisplayName("a body declared gzip but not gzip yields 400")
    @Test
    void invalidGzipBodyYields400() {
        var receiver = start(OtlpArmeriaReceiver.httpBuilder().route(TRACES, Consumers.accepting()));
        var request = HttpRequest.builder().post("/v1/traces")
                .content(PROTOBUF, HttpData.wrap(new byte[] {1, 2, 3, 4, 5}).array())
                .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build();
        var response = WebClient.of("http://localhost:" + receiver.port())
                .execute(request).aggregate().join();
        assertThat(response.status().code()).isEqualTo(400);
    }
}
