package dev.nthings.otlp4j.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/// How [OtlpExporterConfig.Builder#endpoint(String)] resolves the port: the URL decides, falling
/// back to the scheme's well-known port rather than the builder's gRPC/HTTP seed.
@DisplayName("OTLP exporter endpoint URL port resolution")
class OtlpExporterConfigEndpointTest {

    @DisplayName("a URL decides the port for both the gRPC and HTTP builders")
    @ParameterizedTest
    @CsvSource({
        "http://collector, 80", "https://ingest.example.com, 443",
        "http://collector:8080, 8080", "https://collector:4318, 4318",
        "https://collector/otlp, 443", "https://[::1], 443", "http://[::1]:9, 9"
    })
    void urlDecidesPort(String url, int expected) {
        assertThat(OtlpExporterConfig.builder(Ports.GRPC).endpoint(url).build().port()).isEqualTo(expected);
        assertThat(OtlpExporterConfig.builder(Ports.HTTP).endpoint(url).build().port()).isEqualTo(expected);
    }

    @DisplayName("a portless URL marks the port explicit, so a later environment URL cannot move it")
    @Test
    void portlessUrlIsExplicit() {
        var cfg = OtlpExporterConfig.builder(Ports.GRPC)
                .endpoint("https://ingest.example.com")
                .fromEnvironment(Map.of(OtlpEnv.ENDPOINT, "http://other:1234")::get)
                .build();

        assertThat(cfg.host()).isEqualTo("ingest.example.com");
        assertThat(cfg.port()).isEqualTo(443);
    }

    @DisplayName("a portless URL keeps its path and unwraps an IPv6 host")
    @Test
    void portlessUrlParts() {
        var cfg = OtlpExporterConfig.builder(Ports.HTTP).endpoint("https://[::1]/otlp").build();

        assertThat(cfg.host()).isEqualTo("::1");
        assertThat(cfg.port()).isEqualTo(443);
        assertThat(cfg.path()).isEqualTo("/otlp");
    }

    @DisplayName("without a URL the seed port is unchanged")
    @Test
    void seedPortWithoutUrl() {
        assertThat(OtlpExporterConfig.builder(Ports.GRPC).build().port()).isEqualTo(4317);
        assertThat(OtlpExporterConfig.builder(Ports.HTTP).build().port()).isEqualTo(4318);
        assertThat(OtlpExporterConfig.builder(Ports.GRPC).endpoint("example", 7).build().port()).isEqualTo(7);
    }
}
