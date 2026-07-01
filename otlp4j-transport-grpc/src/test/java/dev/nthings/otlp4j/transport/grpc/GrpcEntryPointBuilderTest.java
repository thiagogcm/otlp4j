package dev.nthings.otlp4j.transport.grpc;

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

/// Exercises every builder knob on the gRPC entry points. No network I/O: the gRPC channel is lazy
/// and the receiver is never started, so each `build()` only wires configuration.
@DisplayName("gRPC entry-point builders")
class GrpcEntryPointBuilderTest {

    @DisplayName("OtlpGrpcExporter builder applies every knob")
    @Test
    void exporterBuilderAppliesEveryKnob() {
        try (var exporter = OtlpGrpcExporter.builder()
                .fromEnvironment()
                .setEndpoint("collector.example", 4317)
                .setTimeout(Duration.ofSeconds(3))
                .setTls(Tls.systemTrust())
                .addHeader("authorization", "Bearer x")
                .setHeaders(Map.of("x-tenant", "acme"))
                .setHeaders(() -> Map.of("x-rotating", "live"))
                .setCompression(Compression.GZIP)
                .setRetryPolicy(RetryPolicy.builder().setMaxAttempts(3).setInitialBackoff(Duration.ofMillis(50)).setMaxBackoff(Duration.ofSeconds(1)).build())
                .build()) {
            assertThat(exporter.traces()).isNotNull();
            assertThat(exporter.metrics()).isNotNull();
            assertThat(exporter.logs()).isNotNull();
            assertThat(exporter.profiles()).isNotNull();
        }
    }

    @DisplayName("OtlpGrpcExporter.setConfig(config) and to(...) build a client")
    @Test
    void exporterFromConfigAndConvenience() {
        var config = ClientConfig.builder().setEndpoint("h", 4317).build();
        try (var fromConfig = OtlpGrpcExporter.builder().setConfig(config).build()) {
            assertThat(fromConfig.traces()).isNotNull();
        }
        try (var convenience = OtlpGrpcExporter.to("h", 4317)) {
            assertThat(convenience.traces()).isNotNull();
        }
    }

    @DisplayName("OtlpGrpcExporter.fromEnvironment() builds a usable exporter")
    @Test
    void exporterFromEnvironmentFactory() {
        try (var exporter = OtlpGrpcExporter.fromEnvironment()) {
            assertThat(exporter.traces()).isNotNull();
        }
    }

    @DisplayName("OtlpGrpcReceiver builder applies every knob")
    @Test
    void receiverBuilderAppliesEveryKnob() {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var receiver = OtlpGrpcReceiver.builder()
                .setConfig(ServerConfig.builder().build())
                .setEndpoint("127.0.0.1", 0)
                .setPort(0)
                .ephemeralPort()
                .setTls(Tls.disabled())
                .setMaxInboundMessageSizeBytes(1024)
                .setMaxConcurrentCallsPerConnection(8)
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

    @DisplayName("OtlpGrpcReceiver builder binds an ephemeral or a specific host:port")
    @Test
    void receiverBuilderBindsPort() {
        assertThat(OtlpGrpcReceiver.builder().ephemeralPort().build().port()).isZero();
        assertThat(OtlpGrpcReceiver.builder().setEndpoint("127.0.0.1", 0).build().port()).isZero();
    }

    @DisplayName("OtlpGrpcExporter.Builder addHeader adds per key and setHeaders replaces all")
    @Test
    void exporterBuilderHeaders() throws Exception {
        var builder = OtlpGrpcExporter.builder()
                .addHeader("k1", "v1")
                .setHeaders(Map.of("k2", "v2", "k3", "v3"));

        var field = OtlpGrpcExporter.Builder.class.getDeclaredField("config");
        field.setAccessible(true);
        var clientConfigBuilder = (ClientConfig.Builder) field.get(builder);
        var c = clientConfigBuilder.build();
        assertThat(c.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k2", "v2", "k3", "v3"));

        var builder2 = OtlpGrpcExporter.builder()
                .addHeader("k1", "v1")
                .addHeader("k2", "v2")
                .addHeader("k3", "v3");

        var clientConfigBuilder2 = (ClientConfig.Builder) field.get(builder2);
        var c2 = clientConfigBuilder2.build();
        assertThat(c2.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }
}
