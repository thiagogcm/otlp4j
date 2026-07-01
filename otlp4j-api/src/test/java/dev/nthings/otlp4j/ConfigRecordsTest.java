package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import io.github.resilience4j.retry.RetryConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
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
        assertThat(c.retry()).isEqualTo(ClientConfig.defaultRetryConfig());
        assertThat(c.retry().getMaxAttempts()).isEqualTo(5);
        assertThat(c.headers()).isEmpty();
        assertThat(c.path()).isEmpty();
        assertThat(c.headerSupplier()).isNull();
        assertThat(c.connectTimeout()).isNull();
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
        assertThat(ClientConfig.defaults()).isEqualTo(ClientConfig.builder().build());
        assertThat(ServerConfig.defaults()).isEqualTo(ServerConfig.builder().build());
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

    @DisplayName("Tls in-memory and SSLContext factories build the byte and context variants")
    @Test
    void tlsInMemoryAndSslContextFactories() throws Exception {
        var cert = "cert".getBytes(StandardCharsets.UTF_8);
        var key = "key".getBytes(StandardCharsets.UTF_8);
        var trust = "trust".getBytes(StandardCharsets.UTF_8);

        assertThat(Tls.trust(trust)).isInstanceOf(Tls.Inline.class);
        var inline = (Tls.Inline) Tls.custom(cert, key, trust);
        assertThat(inline.cert()).containsExactly(cert);
        assertThat(inline.key()).containsExactly(key);
        assertThat(inline.trust()).containsExactly(trust);
        // Arrays are copied on construction, so a later mutation of the caller's array is not seen.
        cert[0] = 'X';
        assertThat(inline.cert()[0]).isEqualTo((byte) 'c');

        var context = SSLContext.getInstance("TLS");
        context.init(null, null, null);
        var trustManager = defaultTrustManager();
        var sslVariant = (Tls.SslContext) Tls.sslContext(context, trustManager);
        assertThat(sslVariant.sslContext()).isSameAs(context);
        assertThat(sslVariant.trustManager()).isSameAs(trustManager);

        // Inline copies its arrays, so two Inline values with equal content are not equal (the
        // record compares its byte[] components by reference, not by content).
        assertThat(Tls.custom(key, key, null)).isNotEqualTo(Tls.custom(key, key, null));
    }

    @DisplayName("Tls.sslContext rejects null arguments")
    @Test
    void tlsSslContextRejectsNulls() throws Exception {
        var context = SSLContext.getInstance("TLS");
        context.init(null, null, null);
        assertThatThrownBy(() -> Tls.sslContext(context, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Tls.sslContext(null, defaultTrustManager()))
                .isInstanceOf(NullPointerException.class);
    }

    private static X509TrustManager defaultTrustManager() throws Exception {
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);
        for (var tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager x) {
                return x;
            }
        }
        throw new IllegalStateException("no X509TrustManager available");
    }

    @DisplayName("ClientConfig builder carries Resilience4j RetryConfig directly")
    @Test
    void clientConfigBuilderCarriesResilienceRetryConfig() {
        var retry = RetryConfig.custom()
                .maxAttempts(4)
                .waitDuration(Duration.ofMillis(100))
                .build();
        var config = ClientConfig.builder().setRetryConfig(retry).build();

        assertThat(config.retry()).isSameAs(retry);
        assertThat(config.toBuilder().build().retry()).isSameAs(retry);
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

    @DisplayName("ClientConfig builder carries a header supplier alongside static headers")
    @Test
    void clientConfigBuilderHeaderSupplier() {
        Supplier<Map<String, String>> supplier = () -> Map.of("authorization", "Bearer live");
        var c = ClientConfig.builder()
                .addHeader("x-tenant", "abc")
                .setHeaders(supplier)
                .build();
        // The supplier is carried without clearing the static headers.
        assertThat(c.headers()).containsEntry("x-tenant", "abc");
        assertThat(c.headerSupplier()).isSameAs(supplier);
    }

    @DisplayName("ClientConfig builder carries and validates a connect timeout")
    @Test
    void clientConfigBuilderConnectTimeout() {
        var c = ClientConfig.builder().setConnectTimeout(Duration.ofSeconds(2)).build();
        assertThat(c.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThatThrownBy(() -> ClientConfig.builder().setConnectTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
