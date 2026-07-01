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
        assertThat(ClientConfig.builder().setPath("otlp").build().path()).isEqualTo("/otlp");
        assertThat(ClientConfig.builder().setPath("/otlp/").build().path()).isEqualTo("/otlp");
        assertThat(ClientConfig.builder().setPath("/").build().path()).isEmpty();
        assertThat(ClientConfig.builder().setPath("  ").build().path()).isEmpty();
        assertThat(ClientConfig.builder().setPath(null).build().path()).isEmpty();
        // path survives a toBuilder() round-trip
        assertThat(ClientConfig.builder().setPath("/otlp").build().toBuilder().build().path())
                .isEqualTo("/otlp");
    }

    @DisplayName("ClientConfig builder applies fluent overrides")
    @Test
    void clientConfigBuilderFluentOverrides() {
        var c = ClientConfig.builder()
                .setEndpoint("collector.example.com", 4318)
                .addHeader("x-tenant", "abc")
                .setCompression(Compression.GZIP)
                .setTimeout(Duration.ofSeconds(3))
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
        assertThatThrownBy(() -> ClientConfig.builder().setTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("setEndpoint(url) parses scheme, host, port, and path prefix")
    @Test
    void clientConfigSetEndpointUrl() {
        var https = ClientConfig.builder().setEndpoint("https://collector.example.com:4317/otlp").build();
        assertThat(https.host()).isEqualTo("collector.example.com");
        assertThat(https.port()).isEqualTo(4317);
        assertThat(https.path()).isEqualTo("/otlp");
        assertThat(https.tls()).isEqualTo(Tls.systemTrust());

        // http is plaintext; an IPv6 literal is unwrapped.
        var http = ClientConfig.builder().setEndpoint("http://[::1]:4318").build();
        assertThat(http.host()).isEqualTo("::1");
        assertThat(http.port()).isEqualTo(4318);
        assertThat(http.tls()).isEqualTo(Tls.disabled());

        // A portless URL keeps the builder's current (protocol-default) port.
        assertThat(ClientConfig.builder().setEndpoint("http://collector").build().port()).isEqualTo(4317);

        // A later setTls wins over the scheme's system-trust default.
        var custom = ClientConfig.builder()
                .setEndpoint("https://collector:4317")
                .setTls(Tls.trust(Path.of("ca.pem")))
                .build();
        assertThat(custom.tls()).isEqualTo(Tls.trust(Path.of("ca.pem")));
    }

    @DisplayName("setEndpoint(url) rejects a missing or non-http scheme")
    @Test
    void clientConfigSetEndpointUrlRejectsBadScheme() {
        assertThatThrownBy(() -> ClientConfig.builder().setEndpoint("collector:4317"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClientConfig.builder().setEndpoint("ftp://collector"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("setCompression(String) is a familiar door for gzip/none")
    @Test
    void clientConfigSetCompressionString() {
        assertThat(ClientConfig.builder().setCompression("gzip").build().compression())
                .isEqualTo(Compression.GZIP);
        assertThat(ClientConfig.builder().setCompression("NONE").build().compression())
                .isEqualTo(Compression.NONE);
        assertThatThrownBy(() -> ClientConfig.builder().setCompression("brotli"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("getDefault returns the all-defaults config")
    @Test
    void configGetDefault() {
        assertThat(ClientConfig.getDefault()).isEqualTo(ClientConfig.builder().build());
        assertThat(ServerConfig.getDefault()).isEqualTo(ServerConfig.builder().build());
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
                .setBindHost("127.0.0.1")
                .setPort(5000)
                .setTls(Tls.systemTrust())
                .setMaxInboundMessageSizeBytes(1024)
                .setMaxConcurrentCallsPerConnection(8)
                .setHandshakeTimeout(Duration.ofSeconds(5))
                .setServerExecutor(executor)
                .build();

        var derived = original.toBuilder().setPort(6000).build();

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
        assertThatThrownBy(() -> ServerConfig.builder().setPort(70000).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("ServerConfig rejects non-positive hardening limits and a non-positive handshake timeout")
    @Test
    void serverConfigRejectsBadHardeningLimits() {
        assertThatThrownBy(() -> ServerConfig.builder().setMaxInboundMessageSizeBytes(0).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ServerConfig.builder().setMaxConcurrentCallsPerConnection(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ServerConfig.builder().setHandshakeTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("RetryPolicy rejects invalid backoffs and accepts ordered ones")
    @Test
    void retryPolicyValidates() {
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        // With retries enabled, backoffs must be positive and ordered - rejected, not clamped.
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ZERO, Duration.ofSeconds(1), 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofMillis(200), Duration.ofMillis(50), 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        // A multiplier below 1 would shrink the backoff - rejected when retries are enabled.
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(1), 0.5))
                .isInstanceOf(IllegalArgumentException.class);
        // No-retry policy ignores backoffs; a well-formed retrying policy is accepted.
        assertThat(RetryPolicy.none()).isEqualTo(new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.5));
        assertThat(new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(1), 1.5).maxAttempts())
                .isEqualTo(3);
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

    @DisplayName("RetryPolicy.builder builds a validated retrying policy with a backoff multiplier")
    @Test
    void retryPolicyBuilderBuildsAValidatedRetryingPolicy() {
        var policy = RetryPolicy.builder()
                .setMaxAttempts(4)
                .setInitialBackoff(Duration.ofMillis(100))
                .setMaxBackoff(Duration.ofSeconds(2))
                .setBackoffMultiplier(2.0)
                .build();
        assertThat(policy.maxAttempts()).isEqualTo(4);
        assertThat(policy.initialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.maxBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.backoffMultiplier()).isEqualTo(2.0);
        // getDefault mirrors the SDK: 5 attempts, 1s to 5s, 1.5x.
        assertThat(RetryPolicy.getDefault().backoffMultiplier()).isEqualTo(1.5);
        assertThatThrownBy(() -> RetryPolicy.builder()
                        .setMaxAttempts(3)
                        .setInitialBackoff(Duration.ofMillis(200))
                        .setMaxBackoff(Duration.ofMillis(50))
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("ClientConfig builder addHeader adds per key and setHeaders replaces all")
    @Test
    void clientConfigBuilderHeaders() {
        var c = ClientConfig.builder()
                .addHeader("k1", "v1")
                .setHeaders(Map.of("k2", "v2", "k3", "v3"))
                .build();
        assertThat(c.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k2", "v2", "k3", "v3"));

        var c2 = ClientConfig.builder()
                .addHeader("k1", "v1")
                .addHeader("k2", "v2")
                .addHeader("k3", "v3")
                .build();
        assertThat(c2.headers()).containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }
}
