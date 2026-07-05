package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.receiver.Receiver;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Drives the OTLP/HTTP receiver with a raw [HttpClient] to exercise the server-side error mapping
/// (bad method, malformed body, oversized body, bad encoding) that the high-level client never emits.
@Timeout(30)
@DisplayName("OTLP/HTTP server error mapping")
class HttpServerErrorsTest {

    private final List<Receiver> receivers = new ArrayList<>();
    private final HttpClient client = HttpClient.newHttpClient();

    @AfterEach
    void teardown() {
        client.close();
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) { /* keep tearing down */ }
        }
    }

    @DisplayName("A non-POST request is rejected with 405")
    @Test
    void nonPostIsRejectedWith405() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/traces")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(405);
    }

    @DisplayName("A malformed protobuf body is rejected with 400")
    @Test
    void malformedProtobufIsRejectedWith400() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        // A bare varint tag with no value: the parser reaches EOF mid-field and throws.
        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/metrics"))
                        .header("Content-Type", "application/x-protobuf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {0x08}))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
    }

    @DisplayName("A JSON Content-Type is rejected with 415 (this receiver is protobuf-only)")
    @Test
    void jsonContentTypeIsRejectedWith415() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/traces"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(415);
        assertThat(response.body()).contains("application/x-protobuf");
    }

    @DisplayName("A text/plain (or any other non-protobuf) Content-Type is rejected with 415")
    @Test
    void unknownContentTypeIsRejectedWith415() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/traces"))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(415);
    }

    @DisplayName("An absent Content-Type header is tolerated (some clients omit it)")
    @Test
    void absentContentTypeIsTolerated() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/traces"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @DisplayName("A protobuf Content-Type is accepted regardless of case or trailing parameters")
    @ParameterizedTest
    @ValueSource(strings = {
            "application/x-protobuf",
            "APPLICATION/X-PROTOBUF",
            "application/x-protobuf; charset=utf-8",
            "application/x-protobuf;charset=utf-8"
    })
    void protobufContentTypeVariantsAreAccepted(String contentType) throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/traces"))
                        .header("Content-Type", contentType)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @DisplayName("A body over the size cap is rejected with 413")
    @Test
    void oversizedBodyIsRejectedWith413() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder().setMaxInboundMessageSizeBytes(16));

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/logs"))
                        .header("Content-Type", "application/x-protobuf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[128]))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(413);
    }

    @DisplayName("A body that claims gzip but is not is rejected with 400")
    @Test
    void badGzipEncodingIsRejectedWith400() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder());

        var response = client.send(
                HttpRequest.newBuilder(uri(port, "/v1/traces"))
                        .header("Content-Type", "application/x-protobuf")
                        .header("Content-Encoding", "gzip")
                        .POST(HttpRequest.BodyPublishers.ofString("not actually gzip"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
    }

    @DisplayName("A throttled rejection maps to 503 with a Retry-After header")
    @Test
    void throttledRejectionCarriesRetryAfter() throws Exception {
        var receiver = OtlpHttpReceiver.builder().ephemeralPort()
                .onTraces(t -> CompletableFuture.completedFuture(
                        ConsumeResult.throttled(Duration.ofSeconds(2), "at capacity")))
                .build()
                .start();
        receivers.add(receiver);

        var response = client.send(emptyTraceRequest(receiver.port()), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.headers().firstValue("Retry-After")).hasValue("2");
    }

    @DisplayName("A retryable rejection without a hint maps to 503 without Retry-After")
    @Test
    void retryableRejectionWithoutHintHasNoRetryAfter() throws Exception {
        var receiver = OtlpHttpReceiver.builder().ephemeralPort()
                .onTraces(t -> CompletableFuture.completedFuture(ConsumeResult.retryable("queue full")))
                .build()
                .start();
        receivers.add(receiver);

        var response = client.send(emptyTraceRequest(receiver.port()), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.headers().firstValue("Retry-After")).isEmpty();
    }

    @DisplayName("Admission control rejects at capacity with 429 and Retry-After, then recovers")
    @Test
    void admissionControlRejectsWith429ThenRecovers() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var gate = new CompletableFuture<ConsumeResult>();
        var receiver = OtlpHttpReceiver.builder().ephemeralPort()
                .setMaxInFlightRequests(1)
                .setThrottleRetryAfter(Duration.ofSeconds(3))
                .onTraces(t -> {
                    entered.countDown();
                    return gate;
                })
                .build()
                .start();
        receivers.add(receiver);
        var request = emptyTraceRequest(receiver.port());

        // First request occupies the single admission slot until the gate opens.
        var first = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var second = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(second.statusCode()).isEqualTo(429);
        assertThat(second.headers().firstValue("Retry-After")).hasValue("3");

        gate.complete(ConsumeResult.accepted());
        assertThat(first.join().statusCode()).isEqualTo(200);
        var third = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(third.statusCode()).isEqualTo(200);
    }

    @DisplayName("An admission rejection without a configured hint carries no Retry-After header")
    @Test
    void admissionRejectionWithoutHintHasNoRetryAfter() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var gate = new CompletableFuture<ConsumeResult>();
        var receiver = OtlpHttpReceiver.builder().ephemeralPort()
                .setMaxInFlightRequests(1)
                .onTraces(t -> {
                    entered.countDown();
                    return gate;
                })
                .build()
                .start();
        receivers.add(receiver);
        var request = emptyTraceRequest(receiver.port());

        var first = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var second = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(second.statusCode()).isEqualTo(429);
        assertThat(second.headers().firstValue("Retry-After")).isEmpty();

        gate.complete(ConsumeResult.accepted());
        assertThat(first.join().statusCode()).isEqualTo(200);
    }

    /// A valid (empty-body) protobuf trace export request.
    private static HttpRequest emptyTraceRequest(int port) {
        return HttpRequest.newBuilder(uri(port, "/v1/traces"))
                .header("Content-Type", "application/x-protobuf")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();
    }

    private int startReceiver(OtlpHttpReceiver.Builder builder) {
        var receiver = builder.ephemeralPort()
                .onTraces(t -> ConsumeResult.acceptedStage())
                .onMetrics(m -> ConsumeResult.acceptedStage())
                .onLogs(l -> ConsumeResult.acceptedStage())
                .build()
                .start();
        receivers.add(receiver);
        return receiver.port();
    }

    private static URI uri(int port, String path) {
        return URI.create("http://localhost:" + port + path);
    }
}
