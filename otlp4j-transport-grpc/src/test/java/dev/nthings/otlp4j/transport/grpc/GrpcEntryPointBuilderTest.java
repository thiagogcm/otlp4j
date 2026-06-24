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
                .endpoint("collector.example", 4317)
                .host("collector.example")
                .port(4317)
                .timeout(Duration.ofSeconds(3))
                .tls(Tls.systemTrust())
                .header("authorization", "Bearer x")
                .headers(Map.of("x-tenant", "acme"))
                .compression(Compression.GZIP)
                .retry(RetryPolicy.exponential(3, Duration.ofMillis(50), Duration.ofSeconds(1)))
                .build()) {
            assertThat(exporter.traces()).isNotNull();
            assertThat(exporter.metrics()).isNotNull();
            assertThat(exporter.logs()).isNotNull();
            assertThat(exporter.profiles()).isNotNull();
        }
    }

    @DisplayName("OtlpGrpcExporter.transport(config) and to(...) build a client")
    @Test
    void exporterFromConfigAndConvenience() {
        var config = ClientConfig.builder().endpoint("h", 4317).build();
        try (var fromConfig = OtlpGrpcExporter.builder().transport(config).build()) {
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
                .transport(ServerConfig.builder().build())
                .endpoint("127.0.0.1", 0)
                .port(0)
                .ephemeralPort()
                .tls(Tls.disabled())
                .maxInboundMessageSizeBytes(1024)
                .maxConcurrentCallsPerConnection(8)
                .handshakeTimeout(Duration.ofSeconds(5))
                .serverExecutor(executor)
                .onTraces(t -> ConsumeResult.acceptedStage())
                .onMetrics(m -> ConsumeResult.acceptedStage())
                .onLogs(l -> ConsumeResult.acceptedStage())
                .onProfiles(p -> ConsumeResult.acceptedStage())
                .build();
        assertThat(receiver.port()).isZero(); // never started
        executor.shutdown();
    }

    @DisplayName("OtlpGrpcReceiver.on(...) convenience builds a receiver")
    @Test
    void receiverConvenienceFactories() {
        assertThat(OtlpGrpcReceiver.on(0).port()).isZero();
        assertThat(OtlpGrpcReceiver.on("127.0.0.1", 0).port()).isZero();
    }

    @DisplayName("OtlpGrpcExporter.Builder headers(map) replaces, and addHeaders(map) merges")
    @Test
    void exporterBuilderHeadersAndAddHeaders() throws Exception {
        var builder = OtlpGrpcExporter.builder()
                .header("k1", "v1")
                .headers(Map.of("k2", "v2", "k3", "v3"));

        var field = OtlpGrpcExporter.Builder.class.getDeclaredField("config");
        field.setAccessible(true);
        var clientConfigBuilder = (ClientConfig.Builder) field.get(builder);
        var c = clientConfigBuilder.build();
        assertThat(c.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k2", "v2", "k3", "v3"));

        var builder2 = OtlpGrpcExporter.builder()
                .header("k1", "v1")
                .addHeaders(Map.of("k2", "v2", "k3", "v3"));

        var clientConfigBuilder2 = (ClientConfig.Builder) field.get(builder2);
        var c2 = clientConfigBuilder2.build();
        assertThat(c2.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }
}
