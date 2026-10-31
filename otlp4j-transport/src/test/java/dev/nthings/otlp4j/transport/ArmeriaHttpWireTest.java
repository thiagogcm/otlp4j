package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.PROFILES;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.profiles.v1development.Profile;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/// OTLP/HTTP status-code and header mapping, driven by a raw Armeria [WebClient] against a real
/// Armeria server so the exact HTTP status and headers can be asserted at the wire.
@Timeout(60)
@DisplayName("Armeria OTLP/HTTP wire mapping")
class ArmeriaHttpWireTest {

    private static final MediaType PROTOBUF = MediaType.parse("application/x-protobuf");
    private static final String PATH = "/v1/traces";

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

    /// A well-formed request whose only sample refers to a stack its dictionary does not have.
    static ExportProfilesServiceRequest danglingProfilesRequest() {
        return ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder().addScopeProfiles(ScopeProfiles.newBuilder()
                        .addProfiles(Profile.newBuilder()
                                .addSamples(Sample.newBuilder().setStackIndex(9).addValues(1L)))))
                .build();
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
        cleanup.receiver(receiver);
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

    @DisplayName("an unreadable profiles version is refused before the body is decoded")
    @Test
    void unreadableProfilesVersionIsRefusedBeforeDecoding() {
        var garbage = new byte[] {(byte) 0x08, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        var request = HttpRequest.builder().post("/v1development/profiles")
                .header("otlp-profiles-development-version", "2")
                .content(PROTOBUF, garbage)
                .build();

        var response = client(OtlpReceiver.httpBuilder().route(PROFILES, Consumers.accepting()))
                .execute(request).aggregate().join();

        assertThat(response.status().code()).isEqualTo(400);
        assertThat(response.contentUtf8()).contains("otlp-profiles-development-version");
    }

    @DisplayName("a dangling dictionary reference is the sender's error")
    @Test
    void danglingReferenceYields400() {
        var profile = new AtomicReference<ProfilesData>();
        var response = client(OtlpReceiver.httpBuilder().route(PROFILES, Consumers.record(PROFILES, profile)))
                .execute(HttpRequest.builder().post("/v1development/profiles")
                        .content(PROTOBUF, danglingProfilesRequest().toByteArray())
                        .build())
                .aggregate().join();

        assertThat(response.status().code()).isEqualTo(400);
    }
}
