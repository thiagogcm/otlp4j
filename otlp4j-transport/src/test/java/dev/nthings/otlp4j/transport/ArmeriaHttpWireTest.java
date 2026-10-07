package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    private final List<OtlpReceiver> receivers = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var receiver : receivers) {
            try {
                receiver.shutdown(Duration.ofSeconds(5)).toCompletableFuture().get(5, TimeUnit.SECONDS);
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
        return client(OtlpReceiver.httpBuilder().route(TRACES, Consumers.accepting()));
    }

    private WebClient client(OtlpReceiver.Builder builder) {
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

    @DisplayName("a non-application media type yields 415")
    @Test
    void textPlainYields415() {
        var response = startAccepting()
                .execute(post(MediaType.PLAIN_TEXT_UTF_8, "hi".getBytes(StandardCharsets.UTF_8)))
                .aggregate().join();
        assertThat(response.status().code()).isEqualTo(415);
    }

    @DisplayName("an oversized gzip body is inflated and capped with 413")
    @Test
    void oversizedGzipBodyYields413() throws Exception {
        var client = client(OtlpReceiver.httpBuilder()
                .maxInboundMessageSizeBytes(16)
                .route(TRACES, Consumers.accepting()));
        var request = HttpRequest.builder().post(PATH)
                .content(PROTOBUF, gzip(traceBytes()))
                .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build();
        assertThat(client.execute(request).aggregate().join().status().code()).isEqualTo(413);
    }

    private static byte[] gzip(byte[] data) throws Exception {
        var out = new ByteArrayOutputStream();
        try (var gz = new GZIPOutputStream(out)) {
            gz.write(data);
        }
        return out.toByteArray();
    }
}
