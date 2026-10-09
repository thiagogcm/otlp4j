package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/// Additional coverage for the header path, the dual-protocol public builder, the exporter's OTLP/HTTP
/// knobs, and the less-common request and status mappings.
@Timeout(60)
@DisplayName("Armeria transport extra coverage")
class ArmeriaExtraCoverageTest {

    private static final MediaType PROTOBUF = MediaType.parse("application/x-protobuf");

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

    private OtlpReceiver start(OtlpReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build().start();
        cleanup.receiver(receiver);
        return receiver;
    }

    private static byte[] traceBytes() {
        return SignalCodec.TRACES.toProto(
                Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER))).toByteArray();
    }

    @DisplayName("the dual-protocol public builder serves gRPC and HTTP on one port")
    @Test
    void dualBuilderServesBothProtocols() {
        var received = new CopyOnWriteArrayList<TracesData>();
        var receiver = start(OtlpReceiver.builder()
                .route(TRACES, Consumers.of(p -> {
                    received.add(p.data());
                    return DeliveryResult.accepted(Consumers.PROV);
                })));

        var grpcExporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port()).timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false)).build().start();
        cleanup.client(grpcExporter);
        var httpExporter = OtlpExporter.httpBuilder()
                .endpoint("localhost", receiver.port()).timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false)).build().start();
        cleanup.client(httpExporter);

        var grpcSent = Fixtures.traceData(Fixtures.span("grpc", Span.Kind.SERVER));
        var httpSent = Fixtures.traceData(Fixtures.span("http", Span.Kind.CLIENT));
        assertThat(grpcExporter.export(TRACES, grpcSent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(httpExporter.export(TRACES, httpSent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received).containsExactly(grpcSent, httpSent);
    }

    @DisplayName("gRPC per-call headers reach the server and the export round-trips")
    @Test
    void grpcHeadersRoundTrip() {
        var received = new AtomicReference<TracesData>();
        var metadata = new AtomicReference<ClientMetadata>();
        var receiver = start(OtlpReceiver.grpcBuilder()
                .route(TRACES, (context, payload) -> {
                    received.set(payload.data());
                    metadata.set(context.metadata());
                    return DeliveryResult.acceptedStage(Consumers.PROV);
                }));
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .addHeader("authorization", "Bearer token")
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
        assertThat(metadata.get().headers()).containsEntry("authorization", List.of("Bearer token"));
    }

    @DisplayName("HTTP per-request headers reach the server alongside the telemetry")
    @Test
    void httpHeadersAndKnobsRoundTrip() {
        var received = new AtomicReference<TracesData>();
        var metadata = new AtomicReference<ClientMetadata>();
        var receiver = start(OtlpReceiver.httpBuilder()
                .route(TRACES, (context, payload) -> {
                    received.set(payload.data());
                    metadata.set(context.metadata());
                    return DeliveryResult.acceptedStage(Consumers.PROV);
                }));
        var exporter = OtlpExporter.httpBuilder()
                .endpoint("localhost", receiver.port())
                .addHeader("authorization", "Bearer token")
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
        assertThat(metadata.get().headers()).containsEntry("authorization", List.of("Bearer token"));
    }

    @DisplayName("a gRPC rejection carrying a cause still maps to UNAVAILABLE")
    @Test
    void grpcRejectionWithCauseMapsToUnavailable() {
        var receiver = start(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.retryableWithCause("downstream flaked", new IllegalStateException("boom"))));
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port()).timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false)).build().start();
        cleanup.client(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @DisplayName("admission at capacity without a throttle hint yields 429 and no Retry-After")
    @Test
    void httpAdmissionNoHintYields429() throws Exception {
        var entered = new CountDownLatch(1);
        var gate = new CompletableFuture<DeliveryResult>();
        var receiver = start(OtlpReceiver.httpBuilder()
                .maxInFlightRequests(1)
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
        var receiver = start(OtlpReceiver.httpBuilder().route(TRACES, Consumers.accepting()));
        var request = HttpRequest.builder().post("/v1/traces")
                .content(PROTOBUF, HttpData.wrap(new byte[] {1, 2, 3, 4, 5}).array())
                .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build();
        var response = WebClient.of("http://localhost:" + receiver.port())
                .execute(request).aggregate().join();
        assertThat(response.status().code()).isEqualTo(400);
    }
}
