package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Drives the OTLP/HTTP receiver with a raw [HttpClient] to exercise the server-side error mapping
/// (bad method, malformed body, oversized body, bad encoding) that the high-level client never emits.
@Timeout(30)
@DisplayName("OTLP/HTTP server error mapping")
class HttpServerErrorsTest {

    private final List<OtlpHttpReceiver> receivers = new ArrayList<>();
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

    @DisplayName("A body over the size cap is rejected with 413")
    @Test
    void oversizedBodyIsRejectedWith413() throws Exception {
        var port = startReceiver(OtlpHttpReceiver.builder().maxInboundMessageSizeBytes(16));

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
