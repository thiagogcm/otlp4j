package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Transport config records")
class ConfigRecordsTest {

    @DisplayName("ClientConfig builder applies sensible defaults")
    @Test
    void clientConfigDefaults() {
        var c = ClientConfig.builder().build();
        assertThat(c.host()).isEqualTo("localhost");
        assertThat(c.port()).isEqualTo(4317);
        assertThat(c.timeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(c.tls()).isEqualTo(Tls.disabled());
        assertThat(c.compression()).isEqualTo(Compression.NONE);
        assertThat(c.retry()).isEqualTo(RetryPolicy.none());
        assertThat(c.headers()).isEmpty();
        assertThat(c.path()).isEmpty();
    }

    @DisplayName("ClientConfig normalizes the endpoint path prefix")
    @Test
    void clientConfigPathNormalization() {
        assertThat(ClientConfig.builder().path("otlp").build().path()).isEqualTo("/otlp");
        assertThat(ClientConfig.builder().path("/otlp/").build().path()).isEqualTo("/otlp");
        assertThat(ClientConfig.builder().path("/").build().path()).isEmpty();
        assertThat(ClientConfig.builder().path("  ").build().path()).isEmpty();
        assertThat(ClientConfig.builder().path(null).build().path()).isEmpty();
        // path survives a toBuilder() round-trip
        assertThat(ClientConfig.builder().path("/otlp").build().toBuilder().build().path())
                .isEqualTo("/otlp");
    }

    @DisplayName("ClientConfig builder applies fluent overrides")
    @Test
    void clientConfigBuilderFluentOverrides() {
        var c = ClientConfig.builder()
                .endpoint("collector.example.com", 4318)
                .header("x-tenant", "abc")
                .compression(Compression.GZIP)
                .timeout(Duration.ofSeconds(3))
                .build();
        assertThat(c.host()).isEqualTo("collector.example.com");
        assertThat(c.port()).isEqualTo(4318);
        assertThat(c.headers()).containsEntry("x-tenant", "abc");
        assertThat(c.compression()).isEqualTo(Compression.GZIP);
        assertThat(c.timeout()).isEqualTo(Duration.ofSeconds(3));
    }

    @DisplayName("ClientConfig rejects a zero timeout")
    @Test
    void clientConfigRejectsZeroTimeout() {
        assertThatThrownBy(() -> ClientConfig.builder().timeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("ServerConfig builder applies sensible defaults")
    @Test
    void serverConfigDefaults() {
        var c = ServerConfig.builder().build();
        assertThat(c.bindHost()).isEqualTo("localhost");
        assertThat(c.port()).isEqualTo(4317);
        assertThat(c.tls()).isEqualTo(Tls.disabled());
        assertThat(c.maxInboundMessageSizeBytes()).isEqualTo(4 * 1024 * 1024);
        assertThat(c.maxConcurrentCallsPerConnection()).isZero();
        assertThat(c.handshakeTimeout()).isEqualTo(Duration.ofSeconds(20));
        assertThat(c.serverExecutor()).isNull();
    }

    @DisplayName("ServerConfig.toBuilder round-trips and preserves untouched fields")
    @Test
    void serverConfigToBuilderRoundTrips() {
        Executor executor = Runnable::run;
        var original = ServerConfig.builder()
                .bindHost("127.0.0.1")
                .port(5000)
                .tls(Tls.systemTrust())
                .maxInboundMessageSizeBytes(1024)
                .maxConcurrentCallsPerConnection(8)
                .handshakeTimeout(Duration.ofSeconds(5))
                .serverExecutor(executor)
                .build();

        var derived = original.toBuilder().port(6000).build();

        assertThat(derived.bindHost()).isEqualTo("127.0.0.1");
        assertThat(derived.tls()).isEqualTo(Tls.systemTrust());
        assertThat(derived.port()).isEqualTo(6000);
        assertThat(derived.maxInboundMessageSizeBytes()).isEqualTo(1024);
        assertThat(derived.maxConcurrentCallsPerConnection()).isEqualTo(8);
        assertThat(derived.handshakeTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(derived.serverExecutor()).isSameAs(executor);
        assertThat(original.port()).isEqualTo(5000);
    }

    @DisplayName("ServerConfig rejects an out-of-range port")
    @Test
    void serverConfigRejectsBadPort() {
        assertThatThrownBy(() -> ServerConfig.builder().port(70000).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("ServerConfig rejects non-positive hardening limits and a non-positive handshake timeout")
    @Test
    void serverConfigRejectsBadHardeningLimits() {
        assertThatThrownBy(() -> ServerConfig.builder().maxInboundMessageSizeBytes(0).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ServerConfig.builder().maxConcurrentCallsPerConnection(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ServerConfig.builder().handshakeTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("RetryPolicy rejects invalid backoffs and accepts ordered ones")
    @Test
    void retryPolicyValidates() {
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        // With retries enabled, backoffs must be positive and ordered - rejected, not clamped.
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofMillis(200), Duration.ofMillis(50)))
                .isInstanceOf(IllegalArgumentException.class);
        // No-retry policy ignores backoffs; a well-formed retrying policy is accepted.
        assertThat(RetryPolicy.none()).isEqualTo(new RetryPolicy(1, Duration.ZERO, Duration.ZERO));
        assertThat(new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(1)).maxAttempts()).isEqualTo(3);
    }

    @DisplayName("Tls factories expose shared singletons and build custom bundles")
    @Test
    void tlsFactoriesExposeSharedSingletonsAndBuildCustomBundles() {
        assertThat(Tls.disabled()).isSameAs(Tls.disabled()).isInstanceOf(Tls.Disabled.class);
        assertThat(Tls.systemTrust()).isSameAs(Tls.systemTrust()).isInstanceOf(Tls.SystemTrust.class);
        assertThat(Tls.trust(Path.of("ca.pem")))
                .isEqualTo(new Tls.Custom(null, null, Path.of("ca.pem")));
        assertThat(Tls.custom(Path.of("c"), Path.of("k"), null))
                .isEqualTo(new Tls.Custom(Path.of("c"), Path.of("k"), null));
    }

    @DisplayName("RetryPolicy.exponential builds a validated retrying policy")
    @Test
    void retryPolicyExponentialBuildsAValidatedRetryingPolicy() {
        var policy = RetryPolicy.exponential(4, Duration.ofMillis(100), Duration.ofSeconds(2));
        assertThat(policy.maxAttempts()).isEqualTo(4);
        assertThat(policy.initialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.maxBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThatThrownBy(() -> RetryPolicy.exponential(3, Duration.ofMillis(200), Duration.ofMillis(50)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("ClientConfig builder headers(map) replaces, and addHeaders(map) merges")
    @Test
    void clientConfigBuilderHeadersAndAddHeaders() {
        var c = ClientConfig.builder()
                .header("k1", "v1")
                .headers(Map.of("k2", "v2", "k3", "v3"))
                .build();
        assertThat(c.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k2", "v2", "k3", "v3"));

        var c2 = ClientConfig.builder()
                .header("k1", "v1")
                .addHeaders(Map.of("k2", "v2", "k3", "v3"))
                .build();
        assertThat(c2.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }
}
