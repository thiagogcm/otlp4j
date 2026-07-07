package dev.nthings.otlp4j.transport.http.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.net.URI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box test that [HttpOtlpClient] prepends the configured endpoint path prefix to the standard
/// per-signal request paths, and leaves them untouched when no prefix is set.
@DisplayName("OTLP/HTTP client endpoint path prefix")
class HttpOtlpClientPathTest {

    private static URI signalUri(ClientConfig config, SignalDescriptor<?> signal) {
        try (var client = new HttpOtlpClient(config)) {
            return client.uri(signal);
        }
    }

    @DisplayName("a path prefix is inserted before the /v1/<signal> paths")
    @Test
    void prefixApplied() {
        var config = ClientConfig.builder()
                .setEndpoint("collector", 4318)
                .setTls(Tls.systemTrust())
                .setPath("/otlp")
                .build();

        assertThat(signalUri(config, SignalDescriptor.TRACES))
                .isEqualTo(URI.create("https://collector:4318/otlp/v1/traces"));
        assertThat(signalUri(config, SignalDescriptor.PROFILES))
                .isEqualTo(URI.create("https://collector:4318/otlp/v1development/profiles"));
    }

    @DisplayName("with no prefix the standard paths are used verbatim over plaintext http")
    @Test
    void noPrefix() {
        var config = ClientConfig.builder().setEndpoint("collector", 4318).build();

        assertThat(signalUri(config, SignalDescriptor.TRACES))
                .isEqualTo(URI.create("http://collector:4318/v1/traces"));
    }
}
