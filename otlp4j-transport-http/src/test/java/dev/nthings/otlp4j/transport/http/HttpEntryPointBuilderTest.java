package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.model.ConsumeResult;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Exercises every builder knob on the HTTP entry points. No network I/O: the JDK HttpClient does
/// not connect on build and the receiver is never started, so each `build()` only wires config.
@DisplayName("HTTP entry-point builders")
class HttpEntryPointBuilderTest {

    @DisplayName("OtlpHttpExporter builder applies every knob")
    @Test
    void exporterBuilderAppliesEveryKnob() {
        try (var exporter = OtlpHttpExporter.builder()
                .fromEnvironment()
                .setEndpoint("collector.example", 4318)
                .setHost("collector.example")
                .setPort(4318)
                .setTimeout(Duration.ofSeconds(3))
                .setTls(Tls.systemTrust())
                .addHeader("authorization", "Bearer x")
                .setHeaders(Map.of("x-tenant", "acme"))
                .setCompression(Compression.GZIP)
                .setRetryPolicy(RetryPolicy.builder().setMaxAttempts(3).setInitialBackoff(Duration.ofMillis(50)).setMaxBackoff(Duration.ofSeconds(1)).build())
                .build()) {
            assertThat(exporter.traces()).isNotNull();
            assertThat(exporter.metrics()).isNotNull();
            assertThat(exporter.logs()).isNotNull();
            assertThat(exporter.profiles()).isNotNull();
        }
    }

    @DisplayName("OtlpHttpExporter.transport(config) and to(...) build a client")
    @Test
    void exporterFromConfigAndConvenience() {
        var config = ClientConfig.builder().setEndpoint("h", 4318).build();
        try (var fromConfig = OtlpHttpExporter.builder().transport(config).build()) {
            assertThat(fromConfig.traces()).isNotNull();
        }
        try (var convenience = OtlpHttpExporter.to("h", 4318)) {
            assertThat(convenience.traces()).isNotNull();
        }
    }

    @DisplayName("OtlpHttpExporter.fromEnvironment() builds a usable exporter")
    @Test
    void exporterFromEnvironmentFactory() {
        try (var exporter = OtlpHttpExporter.fromEnvironment()) {
            assertThat(exporter.traces()).isNotNull();
        }
    }

    @DisplayName("OtlpHttpExporter.Builder path(...) sets the endpoint path prefix on the config")
    @Test
    void exporterBuilderPath() throws Exception {
        var builder = OtlpHttpExporter.builder().setEndpoint("h", 4318).setPath("/otlp");

        var field = OtlpHttpExporter.Builder.class.getDeclaredField("config");
        field.setAccessible(true);
        var c = ((ClientConfig.Builder) field.get(builder)).build();
        assertThat(c.path()).isEqualTo("/otlp");
    }

    @DisplayName("OtlpHttpReceiver builder applies every knob")
    @Test
    void receiverBuilderAppliesEveryKnob() {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var receiver = OtlpHttpReceiver.builder()
                .transport(ServerConfig.builder().build())
                .setEndpoint("127.0.0.1", 0)
                .setPort(0)
                .ephemeralPort()
                .setTls(Tls.disabled())
                .setMaxInboundMessageSizeBytes(1024)
                .setHandshakeTimeout(Duration.ofSeconds(5))
                .setServerExecutor(executor)
                .onTraces(t -> ConsumeResult.acceptedStage())
                .onMetrics(m -> ConsumeResult.acceptedStage())
                .onLogs(l -> ConsumeResult.acceptedStage())
                .onProfiles(p -> ConsumeResult.acceptedStage())
                .build();
        assertThat(receiver.port()).isZero(); // never started
        executor.shutdown();
    }

    @DisplayName("OtlpHttpReceiver.on(...) convenience builds a receiver")
    @Test
    void receiverConvenienceFactories() {
        assertThat(OtlpHttpReceiver.on(0).port()).isZero();
        assertThat(OtlpHttpReceiver.on("127.0.0.1", 0).port()).isZero();
    }

    @DisplayName("OtlpHttpExporter.Builder addHeader adds per key and setHeaders replaces all")
    @Test
    void exporterBuilderHeaders() throws Exception {
        var builder = OtlpHttpExporter.builder()
                .addHeader("k1", "v1")
                .setHeaders(Map.of("k2", "v2", "k3", "v3"));

        var field = OtlpHttpExporter.Builder.class.getDeclaredField("config");
        field.setAccessible(true);
        var clientConfigBuilder = (ClientConfig.Builder) field.get(builder);
        var c = clientConfigBuilder.build();
        assertThat(c.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k2", "v2", "k3", "v3"));

        var builder2 = OtlpHttpExporter.builder()
                .addHeader("k1", "v1")
                .addHeader("k2", "v2")
                .addHeader("k3", "v3");

        var clientConfigBuilder2 = (ClientConfig.Builder) field.get(builder2);
        var c2 = clientConfigBuilder2.build();
        assertThat(c2.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }
}
