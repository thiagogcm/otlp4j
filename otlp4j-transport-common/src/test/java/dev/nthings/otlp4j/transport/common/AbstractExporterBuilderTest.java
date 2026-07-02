package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.spi.OtlpClient;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.jspecify.annotations.Nullable;

/// Exercises the shared exporter-builder base through a test subtype whose `newClient` captures
/// the built [ClientConfig] - no reflection into builder internals.
@DisplayName("AbstractExporterBuilder")
class AbstractExporterBuilderTest {

    /// Minimal concrete builder: records the config handed to `newClient`.
    private static final class TestBuilder extends AbstractExporterBuilder<TestBuilder> {

        @Nullable ClientConfig captured;

        TestBuilder() {
            super(ClientConfig.builder(), TestBuilder.class.getName());
        }

        @Override
        protected TestBuilder self() {
            return this;
        }

        @Override
        protected OtlpClient newClient(ClientConfig config) {
            this.captured = config;
            return new RecordingOtlpClient();
        }
    }

    @DisplayName("every shared setter lands on the built ClientConfig")
    @Test
    void sharedSettersLandOnConfig() {
        var tls = Tls.systemTrust();
        var retry = RetryConfig.custom().maxAttempts(2).waitDuration(Duration.ofMillis(10)).build();
        Supplier<Map<String, String>> supplier = () -> Map.of("x-rotating", "live");
        var builder = new TestBuilder();
        try (var exporter = builder
                .setEndpoint("collector.example", 4317)
                .setTimeout(Duration.ofSeconds(3))
                .setTls(tls)
                .addHeader("authorization", "Bearer x")
                .setHeaders(supplier)
                .setCompression(Compression.GZIP)
                .setRetryConfig(retry)
                .build()) {
            assertThat(exporter.traces()).isNotNull();
        }
        var c = builder.captured;
        assertThat(c).isNotNull();
        assertThat(c.host()).isEqualTo("collector.example");
        assertThat(c.port()).isEqualTo(4317);
        assertThat(c.timeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(c.tls()).isSameAs(tls);
        assertThat(c.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("authorization", "Bearer x"));
        assertThat(c.headerSupplier()).isSameAs(supplier);
        assertThat(c.compression()).isEqualTo(Compression.GZIP);
        assertThat(c.retry()).isSameAs(retry);
    }

    @DisplayName("setEndpoint(url) applies host, port, path prefix, and TLS mode")
    @Test
    void urlEndpointAppliesAllParts() {
        var builder = new TestBuilder();
        builder.setEndpoint("https://collector.example:4318/otlp").build().close();
        var c = builder.captured;
        assertThat(c).isNotNull();
        assertThat(c.host()).isEqualTo("collector.example");
        assertThat(c.port()).isEqualTo(4318);
        assertThat(c.path()).isEqualTo("/otlp");
        assertThat(c.tls()).isEqualTo(Tls.systemTrust());
    }

    @DisplayName("addHeader accumulates per key and setHeaders(Map) replaces all")
    @Test
    void headerAccumulationAndReplacement() {
        var accumulating = new TestBuilder();
        accumulating.addHeader("k1", "v1").addHeader("k2", "v2").addHeader("k3", "v3").build().close();
        assertThat(accumulating.captured).isNotNull();
        assertThat(accumulating.captured.headers())
                .containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));

        var replacing = new TestBuilder();
        replacing.addHeader("k1", "v1").setHeaders(Map.of("k2", "v2", "k3", "v3")).build().close();
        assertThat(replacing.captured).isNotNull();
        assertThat(replacing.captured.headers())
                .containsExactlyInAnyOrderEntriesOf(Map.of("k2", "v2", "k3", "v3"));
    }

    @DisplayName("setConfig replaces prior state and later setters still override")
    @Test
    void setConfigReplacesThenSettersOverride() {
        var replacement = ClientConfig.builder()
                .setEndpoint("replaced.example", 9999)
                .setCompression(Compression.GZIP)
                .build();
        var builder = new TestBuilder();
        builder.setEndpoint("ignored.example", 1)
                .setConfig(replacement)
                .setTimeout(Duration.ofSeconds(7))
                .build()
                .close();
        var c = builder.captured;
        assertThat(c).isNotNull();
        assertThat(c.host()).isEqualTo("replaced.example");
        assertThat(c.port()).isEqualTo(9999);
        assertThat(c.timeout()).isEqualTo(Duration.ofSeconds(7));
        assertThat(c.compression()).isEqualTo(Compression.GZIP);
    }

    @DisplayName("fromEnvironment() builds a usable exporter")
    @Test
    void fromEnvironmentSmoke() {
        var builder = new TestBuilder();
        try (var exporter = builder.fromEnvironment().build()) {
            assertThat(exporter.logs()).isNotNull();
        }
        assertThat(builder.captured).isNotNull();
    }
}
