package dev.nthings.otlp4j.transport.otlp;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// OTLP/HTTP status-code and header mapping, driven by a raw Armeria [WebClient] against a real
/// Armeria server so the exact HTTP status and headers can be asserted at the wire.
@Timeout(60)
@DisplayName("Armeria OTLP/HTTP wire mapping")
class ArmeriaHttpWireTest {

    private static final MediaType PROTOBUF = MediaType.parse("application/x-protobuf");
    private static final String PATH = "/v1/traces";

    private final List<dev.nthings.otlp4j.transport.common.ServerReceiver> receivers = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    private static byte[] traceBytes() {
        return SignalCodec.TRACES.toProto(
                Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER))).toByteArray();
    }

    private WebClient startAccepting() {
        return client(OtlpArmeriaReceiver.httpBuilder().route(TRACES, Consumers.accepting()));
    }

    private WebClient client(OtlpArmeriaReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build().start();
        receivers.add(receiver);
        return WebClient.of("http://localhost:" + receiver.port());
    }

    private static HttpRequest post(MediaType contentType, byte[] body) {
        return HttpRequest.builder().post(PATH).content(contentType, body).build();
    }

    @DisplayName("a well-formed protobuf POST is accepted with 200")
    @Test
    void wellFormedPostAccepted() {
        var response = startAccepting().execute(post(PROTOBUF, traceBytes())).aggregate().join();
        assertThat(response.status().code()).isEqualTo(200);
        assertThat(response.contentType()).isNotNull();
        assertThat(response.contentType().nameWithoutParameters()).isEqualTo("application/x-protobuf");
    }

    @DisplayName("a missing Content-Type is tolerated")
    @Test
    void missingContentTypeTolerated() {
        var request = HttpRequest.of(
                RequestHeaders.of(HttpMethod.POST, PATH), HttpData.wrap(traceBytes()));
        assertThat(startAccepting().execute(request).aggregate().join().status().code()).isEqualTo(200);
    }

    @DisplayName("partial success is encoded in the 200 response body")
    @Test
    void partialSuccessEncodedInBody() throws Exception {
        var client = client(OtlpArmeriaReceiver.httpBuilder()
                .route(TRACES, Consumers.partial(3L, "3 rejected")));
        var response = client.execute(post(PROTOBUF, traceBytes())).aggregate().join();

        assertThat(response.status().code()).isEqualTo(200);
        var parsed = ExportTraceServiceResponse.parseFrom(response.content().array());
        assertThat(parsed.getPartialSuccess().getRejectedSpans()).isEqualTo(3L);
    }

    @DisplayName("application/json yields 415")
    @Test
    void jsonYields415() {
        var response = startAccepting()
                .execute(post(MediaType.JSON_UTF_8, "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .aggregate().join();
        assertThat(response.status().code()).isEqualTo(415);
    }

    @DisplayName("a non-application media type yields 415")
    @Test
    void textPlainYields415() {
        var response = startAccepting()
                .execute(post(MediaType.PLAIN_TEXT_UTF_8, "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .aggregate().join();
        assertThat(response.status().code()).isEqualTo(415);
    }

    @DisplayName("a GET yields 405")
    @Test
    void getYields405() {
        assertThat(startAccepting().get(PATH).aggregate().join().status().code()).isEqualTo(405);
    }

    @DisplayName("malformed protobuf yields 400")
    @Test
    void malformedProtobufYields400() {
        var response = startAccepting()
                .execute(post(PROTOBUF, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff}))
                .aggregate().join();
        assertThat(response.status().code()).isEqualTo(400);
    }

    @DisplayName("an oversized body yields 413")
    @Test
    void oversizedBodyYields413() {
        var client = client(OtlpArmeriaReceiver.httpBuilder()
                .setMaxInboundMessageSizeBytes(16)
                .route(TRACES, Consumers.accepting()));
        assertThat(client.execute(post(PROTOBUF, traceBytes())).aggregate().join().status().code())
                .isEqualTo(413);
    }

    @DisplayName("an oversized gzip body is inflated and capped with 413")
    @Test
    void oversizedGzipBodyYields413() throws Exception {
        var client = client(OtlpArmeriaReceiver.httpBuilder()
                .setMaxInboundMessageSizeBytes(16)
                .route(TRACES, Consumers.accepting()));
        var request = HttpRequest.builder().post(PATH)
                .content(PROTOBUF, gzip(traceBytes()))
                .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build();
        assertThat(client.execute(request).aggregate().join().status().code()).isEqualTo(413);
    }

    @DisplayName("a gzip body under the cap is inflated and accepted with 200")
    @Test
    void gzipBodyAccepted() throws Exception {
        var request = HttpRequest.builder().post(PATH)
                .content(PROTOBUF, gzip(traceBytes()))
                .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build();
        assertThat(startAccepting().execute(request).aggregate().join().status().code()).isEqualTo(200);
    }

    @DisplayName("a throttled rejection yields 503 with a Retry-After header")
    @Test
    void throttledYields503WithRetryAfter() {
        var client = client(OtlpArmeriaReceiver.httpBuilder()
                .route(TRACES, Consumers.throttled(java.time.Duration.ofSeconds(4), "slow down")));
        var response = client.execute(post(PROTOBUF, traceBytes())).aggregate().join();

        assertThat(response.status().code()).isEqualTo(503);
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER)).isEqualTo("4");
    }

    @DisplayName("admission at capacity yields 429 with a Retry-After header, then recovers")
    @Test
    void admissionAtCapacityYields429() throws Exception {
        var entered = new CountDownLatch(1);
        var gate = new CompletableFuture<DeliveryResult>();
        var client = client(OtlpArmeriaReceiver.httpBuilder()
                .setMaxInFlightRequests(1)
                .setThrottleRetryAfter(java.time.Duration.ofSeconds(3))
                .route(TRACES, Consumers.gated(entered::countDown, gate)));

        var held = client.execute(post(PROTOBUF, traceBytes())).aggregate();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var second = client.execute(post(PROTOBUF, traceBytes())).aggregate().join();
        assertThat(second.status().code()).isEqualTo(429);
        assertThat(second.headers().get(HttpHeaderNames.RETRY_AFTER)).isEqualTo("3");

        gate.complete(DeliveryResult.accepted(Consumers.PROV));
        assertThat(held.get(5, TimeUnit.SECONDS).status().code()).isEqualTo(200);
    }

    private static byte[] gzip(byte[] data) throws Exception {
        var out = new ByteArrayOutputStream();
        try (var gz = new GZIPOutputStream(out)) {
            gz.write(data);
        }
        return out.toByteArray();
    }
}
