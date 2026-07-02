package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import io.github.resilience4j.retry.RetryConfig;
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
                .setPath("/otlp")
                .setTimeout(Duration.ofSeconds(3))
                .setConnectTimeout(Duration.ofSeconds(2))
                .setTls(Tls.systemTrust())
                .addHeader("authorization", "Bearer x")
                .setHeaders(Map.of("x-tenant", "acme"))
                .setHeaders(() -> Map.of("x-rotating", "live"))
                .setCompression(Compression.GZIP)
                .setRetryConfig(RetryConfig.custom().maxAttempts(3).waitDuration(Duration.ofMillis(50)).build())
                .build()) {
            assertThat(exporter.traces()).isNotNull();
            assertThat(exporter.metrics()).isNotNull();
            assertThat(exporter.logs()).isNotNull();
            assertThat(exporter.profiles()).isNotNull();
        }
    }

    @DisplayName("OtlpHttpExporter.setConfig(config) and to(...) build a client")
    @Test
    void exporterFromConfigAndConvenience() {
        var config = ClientConfig.builder().setEndpoint("h", 4318).build();
        try (var fromConfig = OtlpHttpExporter.builder().setConfig(config).build()) {
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

    @DisplayName("OtlpHttpReceiver builder applies every knob")
    @Test
    void receiverBuilderAppliesEveryKnob() {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var receiver = OtlpHttpReceiver.builder()
                .setConfig(ServerConfig.builder().build())
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

    @DisplayName("OtlpHttpReceiver builder binds an ephemeral or a specific host:port")
    @Test
    void receiverBuilderBindsPort() {
        assertThat(OtlpHttpReceiver.builder().ephemeralPort().build().port()).isZero();
        assertThat(OtlpHttpReceiver.builder().setEndpoint("127.0.0.1", 0).build().port()).isZero();
    }

}
