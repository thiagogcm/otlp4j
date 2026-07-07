package dev.nthings.otlp4j.transport.http;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.LOGS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.PROFILES;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
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
                .setRetryPolicy(new RetryPolicy(3, new BackoffPolicy(Duration.ofMillis(50), 1.0, 0.0, Duration.ofMillis(50)), true))
                .build()) {
            assertThat(exporter.consumer(TRACES)).isNotNull();
            assertThat(exporter.consumer(METRICS)).isNotNull();
            assertThat(exporter.consumer(LOGS)).isNotNull();
            assertThat(exporter.consumer(PROFILES)).isNotNull();
        }
    }

    @DisplayName("OtlpHttpExporter.setConfig(config) and to(...) build a client")
    @Test
    void exporterFromConfigAndConvenience() {
        var config = ClientConfig.builder().setEndpoint("h", 4318).build();
        try (var fromConfig = OtlpHttpExporter.builder().setConfig(config).build()) {
            assertThat(fromConfig.consumer(TRACES)).isNotNull();
        }
        try (var convenience = OtlpHttpExporter.to("h", 4318)) {
            assertThat(convenience.consumer(TRACES)).isNotNull();
        }
    }

    @DisplayName("OtlpHttpExporter.fromEnvironment() builds a usable exporter")
    @Test
    void exporterFromEnvironmentFactory() {
        try (var exporter = OtlpHttpExporter.fromEnvironment()) {
            assertThat(exporter.consumer(TRACES)).isNotNull();
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
                .route(TRACES, Consumers.accepting())
                .route(METRICS, Consumers.accepting())
                .route(LOGS, Consumers.accepting())
                .route(PROFILES, Consumers.accepting())
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
