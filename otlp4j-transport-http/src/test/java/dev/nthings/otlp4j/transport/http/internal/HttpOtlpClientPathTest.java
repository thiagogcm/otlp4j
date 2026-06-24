package dev.nthings.otlp4j.transport.http.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Tls;
import java.net.URI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box test that [HttpOtlpClient] prepends the configured endpoint path prefix to the standard
/// per-signal request paths, and leaves them untouched when no prefix is set.
@DisplayName("OTLP/HTTP client endpoint path prefix")
class HttpOtlpClientPathTest {

    private static URI signalUri(ClientConfig config, String field) throws Exception {
        try (var client = new HttpOtlpClient(config)) {
            var f = HttpOtlpClient.class.getDeclaredField(field);
            f.setAccessible(true);
            return (URI) f.get(client);
        }
    }

    @DisplayName("a path prefix is inserted before the /v1/<signal> paths")
    @Test
    void prefixApplied() throws Exception {
        var config = ClientConfig.builder()
                .endpoint("collector", 4318)
                .tls(Tls.systemTrust())
                .path("/otlp")
                .build();

        assertThat(signalUri(config, "tracesUri"))
                .isEqualTo(URI.create("https://collector:4318/otlp/v1/traces"));
        assertThat(signalUri(config, "profilesUri"))
                .isEqualTo(URI.create("https://collector:4318/otlp/v1development/profiles"));
    }

    @DisplayName("with no prefix the standard paths are used verbatim over plaintext http")
    @Test
    void noPrefix() throws Exception {
        var config = ClientConfig.builder().endpoint("collector", 4318).build();

        assertThat(signalUri(config, "tracesUri"))
                .isEqualTo(URI.create("http://collector:4318/v1/traces"));
    }
}
