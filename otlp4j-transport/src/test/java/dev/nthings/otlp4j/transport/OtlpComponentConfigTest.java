package dev.nthings.otlp4j.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import java.time.Duration;
import java.util.HashMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Validate configuration inputs and preserve explicit builder settings.
@DisplayName("OTLP component configs")
class OtlpComponentConfigTest {

    @DisplayName("the exporter builder carries every knob onto the record")
    @Test
    void exporterBuilderCarriesKnobs() {
        var retry = new RetryPolicy(2, BackoffPolicy.defaults(), false);
        var config = OtlpExporterConfig.defaults(4318)
                .toBuilder()
                .endpoint("collector", 4319)
                .path("/otlp")
                .tls(Tls.systemTrust())
                .addHeader("authorization", "Bearer t")
                .compression(Compression.GZIP)
                .retryPolicy(retry)
                .timeoutPolicy(new TimeoutPolicy(Duration.ofSeconds(3), Duration.ofSeconds(15)))
                .build();

        assertThat(config.host()).isEqualTo("collector");
        assertThat(config.port()).isEqualTo(4319);
        assertThat(config.path()).isEqualTo("/otlp");
        assertThat(config.tls()).isEqualTo(Tls.systemTrust());
        assertThat(config.compression()).isEqualTo(Compression.GZIP);
        assertThat(config.headers()).containsEntry("authorization", "Bearer t");
        assertThat(config.retry()).isEqualTo(retry);
        assertThat(config.timeout().perAttempt()).isEqualTo(Duration.ofSeconds(3));
        assertThat(config.timeout().total()).isEqualTo(Duration.ofSeconds(15));
    }

    @DisplayName("an unset total timeout falls back to the per-attempt bound")
    @Test
    void totalTimeoutFallsBackToPerAttempt() {
        var config = OtlpExporterConfig.builder(4317).timeout(Duration.ofSeconds(7)).build();
        assertThat(config.timeout().perAttempt()).isEqualTo(Duration.ofSeconds(7));
        assertThat(config.timeout().total()).isEqualTo(Duration.ofSeconds(7));
    }

    @DisplayName("exporter config rejects an out-of-range port and defensively copies headers")
    @Test
    void exporterConfigValidates() {
        assertThatThrownBy(() -> OtlpExporterConfig.defaults(70000))
                .isInstanceOf(IllegalArgumentException.class);
        var headers = new HashMap<String, String>();
        headers.put("a", "b");
        var config = OtlpExporterConfig.builder(4317).headers(headers).build();
        headers.put("c", "d");
        assertThat(config.headers()).containsOnlyKeys("a");
        assertThatThrownBy(() -> OtlpExporterConfig.builder(4317)
                        .maxRequestBytes((long) Integer.MAX_VALUE + 1)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRequestBytes");
        assertThatThrownBy(() -> OtlpExporterConfig.builder(4317)
                        .maxResponseBytes((long) Integer.MAX_VALUE + 1)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxResponseBytes");
    }

    @DisplayName("the receiver builder carries every knob onto the record")
    @Test
    void receiverBuilderCarriesKnobs() {
        var config = OtlpReceiverConfig.builder(4317)
                .bindHost("0.0.0.0")
                .port(9999)
                .maxInboundMessageSizeBytes(1024)
                .maxClientTimeout(Duration.ofSeconds(7))
                .maxInFlightRequests(8)
                .maxInFlightBytes(2048L)
                .throttleRetryAfter(Duration.ofSeconds(1))
                .build();
        assertThat(config.bindHost()).isEqualTo("0.0.0.0");
        assertThat(config.port()).isEqualTo(9999);
        assertThat(config.maxInboundMessageSizeBytes()).isEqualTo(1024);
        assertThat(config.maxClientTimeout()).isEqualTo(Duration.ofSeconds(7));
        assertThat(config.maxInFlightRequests()).isEqualTo(8);
        assertThat(config.maxInFlightBytes()).isEqualTo(2048L);
        assertThat(config.throttleRetryAfter()).isEqualTo(Duration.ofSeconds(1));
    }

    @DisplayName("the ephemeral-port copy binds port 0 and keeps every other field")
    @Test
    void receiverEphemeralCopy() {
        var config = new OtlpReceiverConfig(
                "0.0.0.0", 4317, Tls.disabled(), ClientAuth.NONE, 1024, Duration.ofSeconds(7), 8, 2048L,
                Duration.ofSeconds(1));
        var ephemeral = config.withEphemeralPort();
        assertThat(ephemeral.port()).isZero();
        assertThat(ephemeral.bindHost()).isEqualTo("0.0.0.0");
        assertThat(ephemeral.maxClientTimeout()).isEqualTo(Duration.ofSeconds(7));
        assertThat(ephemeral.maxInFlightRequests()).isEqualTo(8);
    }

    @DisplayName("receiver config rejects an out-of-range port, a null bind host, and bad limits")
    @Test
    void receiverConfigValidates() {
        assertThatThrownBy(() -> OtlpReceiverConfig.defaults(-1))
                .isInstanceOf(IllegalArgumentException.class);
        var timeout = OtlpReceiverConfig.DEFAULT_MAX_CLIENT_TIMEOUT;
        assertThatThrownBy(() -> new OtlpReceiverConfig(
                        null, 0, Tls.disabled(), ClientAuth.NONE, 1, timeout, 0, 0, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OtlpReceiverConfig(
                        "localhost", 0, Tls.disabled(), ClientAuth.NONE, 0, timeout, 0, 0, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OtlpReceiverConfig(
                        "localhost", 0, Tls.disabled(), ClientAuth.NONE, 1, null, 0, 0, null))
                .isInstanceOf(NullPointerException.class);
        for (var bad : new Duration[] {Duration.ZERO, Duration.ofSeconds(-1)}) {
            assertThatThrownBy(() -> OtlpReceiverConfig.builder(4317).maxClientTimeout(bad).build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxClientTimeout");
        }
    }
}
