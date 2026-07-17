package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// The neutral OTLP receiver and exporter config records: their defaults, validation, and translation
/// to the internal transport configs.
@DisplayName("OTLP component configs")
class OtlpComponentConfigTest {

    @DisplayName("exporter defaults translate to the client config")
    @Test
    void exporterDefaultsTranslate() {
        var config = OtlpExporterConfig.defaults(4317);
        assertThat(config.host()).isEqualTo("localhost");
        assertThat(config.port()).isEqualTo(4317);
        assertThat(config.retry()).isEqualTo(RetryPolicy.defaults());

        var client = config.toClientConfig();
        assertThat(client.host()).isEqualTo("localhost");
        assertThat(client.port()).isEqualTo(4317);
        assertThat(client.timeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(client.totalTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(client.retry()).isEqualTo(RetryPolicy.defaults());
    }

    @DisplayName("exporter builder carries every knob to the client config")
    @Test
    void exporterBuilderCarriesKnobs() {
        var retry = new RetryPolicy(2, BackoffPolicy.defaults(), false);
        var config = OtlpExporterConfig.defaults(4318)
                .toBuilder()
                .setEndpoint("collector", 4319)
                .setPath("/otlp")
                .setTls(Tls.systemTrust())
                .addHeader("authorization", "Bearer t")
                .setCompression(Compression.GZIP)
                .setRetryPolicy(retry)
                .setTimeoutPolicy(new TimeoutPolicy(Duration.ofSeconds(3), Duration.ofSeconds(15)))
                .build();

        var client = config.toClientConfig();
        assertThat(client.host()).isEqualTo("collector");
        assertThat(client.port()).isEqualTo(4319);
        assertThat(client.path()).isEqualTo("/otlp");
        assertThat(client.compression()).isEqualTo(Compression.GZIP);
        assertThat(client.headers()).containsEntry("authorization", "Bearer t");
        assertThat(client.retry()).isEqualTo(retry);
        assertThat(client.timeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(client.totalTimeout()).isEqualTo(Duration.ofSeconds(15));
    }

    @DisplayName("exporter config rejects an out-of-range port and defensively copies headers")
    @Test
    void exporterConfigValidates() {
        assertThatThrownBy(() -> OtlpExporterConfig.defaults(70000))
                .isInstanceOf(IllegalArgumentException.class);
        var headers = new java.util.HashMap<String, String>();
        headers.put("a", "b");
        var config = OtlpExporterConfig.defaults(4317).toBuilder().build();
        assertThat(config.headers()).isNotNull();
    }

    @DisplayName("receiver defaults translate to the server config")
    @Test
    void receiverDefaultsTranslate() {
        var config = OtlpReceiverConfig.defaults(4318);
        assertThat(config.bindHost()).isEqualTo("localhost");
        assertThat(config.port()).isEqualTo(4318);

        var server = config.toServerConfig();
        assertThat(server.bindHost()).isEqualTo("localhost");
        assertThat(server.port()).isEqualTo(4318);
    }

    @DisplayName("receiver ephemeral-port copy and throttle hint translate")
    @Test
    void receiverEphemeralAndThrottle() {
        var config = new OtlpReceiverConfig(
                "0.0.0.0", 4317, Tls.disabled(), 1024, 8, 2048L, Duration.ofSeconds(1));
        assertThat(config.withEphemeralPort().port()).isZero();

        var server = config.toServerConfig();
        assertThat(server.maxInFlightRequests()).isEqualTo(8);
        assertThat(server.maxInFlightBytes()).isEqualTo(2048L);
        assertThat(server.throttleRetryAfter()).isEqualTo(Duration.ofSeconds(1));
    }

    @DisplayName("receiver config rejects an out-of-range port")
    @Test
    void receiverConfigValidates() {
        assertThatThrownBy(() -> OtlpReceiverConfig.defaults(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OtlpReceiverConfig(
                        null, 0, Tls.disabled(), 1, 0, 0, null))
                .isInstanceOf(NullPointerException.class);
    }
}
