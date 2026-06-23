package dev.nthings.otlp4j.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Drives the package-private [ClientConfig.Builder#fromEnvironment] seam with a
/// map-backed lookup, so the suite exercises env parsing without touching process environment.
@DisplayName("OTLP environment configuration")
class OtlpEnvTest {

    private static ClientConfig fromEnv(Map<String, String> env) {
        return ClientConfig.builder().fromEnvironment(env::get).build();
    }

    @DisplayName("http endpoint maps to host/port with plaintext TLS")
    @Test
    void httpEndpoint() {
        var cfg = fromEnv(Map.of(OtlpEnv.ENDPOINT, "http://collector:4318"));

        assertThat(cfg.host()).isEqualTo("collector");
        assertThat(cfg.port()).isEqualTo(4318);
        assertThat(cfg.tls()).isEqualTo(Tls.disabled());
    }

    @DisplayName("https endpoint without cert vars uses system trust")
    @Test
    void httpsEndpointSystemTrust() {
        var cfg = fromEnv(Map.of(OtlpEnv.ENDPOINT, "https://collector:4317"));

        assertThat(cfg.tls()).isEqualTo(Tls.systemTrust());
    }

    @DisplayName("endpoint without a port defaults to 4317")
    @Test
    void endpointDefaultPort() {
        assertThat(fromEnv(Map.of(OtlpEnv.ENDPOINT, "http://collector")).port()).isEqualTo(4317);
        assertThat(fromEnv(Map.of(OtlpEnv.ENDPOINT, "https://collector:443")).port()).isEqualTo(443);
    }

    @DisplayName("a scheme-less, bad-scheme, or blank endpoint is rejected")
    @Test
    void endpointRejections() {
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.ENDPOINT, "collector:4317")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.ENDPOINT, "ftp://collector")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.ENDPOINT, "   ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("an empty endpoint value is treated as unset")
    @Test
    void emptyEndpointIsUnset() {
        var cfg = fromEnv(Map.of(OtlpEnv.ENDPOINT, ""));

        assertThat(cfg.host()).isEqualTo("localhost");
        assertThat(cfg.port()).isEqualTo(4317);
    }

    @DisplayName("an IPv6 endpoint host is unwrapped from its brackets")
    @Test
    void ipv6Endpoint() {
        var cfg = fromEnv(Map.of(OtlpEnv.ENDPOINT, "https://[::1]:4317"));

        assertThat(cfg.host()).isEqualTo("::1");
        assertThat(cfg.port()).isEqualTo(4317);
        assertThat(cfg.tls()).isEqualTo(Tls.systemTrust());
    }

    @DisplayName("https + CERTIFICATE trusts that CA file")
    @Test
    void httpsWithCertificate() {
        var cfg = fromEnv(Map.of(
                OtlpEnv.ENDPOINT, "https://collector:4317",
                OtlpEnv.CERTIFICATE, "/ca.pem"));

        assertThat(cfg.tls()).isEqualTo(Tls.trust(Path.of("/ca.pem")));
    }

    @DisplayName("https + client cert/key (and CA) builds mutual TLS")
    @Test
    void httpsWithClientCertKey() {
        var cfg = fromEnv(Map.of(
                OtlpEnv.ENDPOINT, "https://collector:4317",
                OtlpEnv.CLIENT_CERTIFICATE, "/client.pem",
                OtlpEnv.CLIENT_KEY, "/client.key",
                OtlpEnv.CERTIFICATE, "/ca.pem"));

        assertThat(cfg.tls())
                .isEqualTo(Tls.custom(Path.of("/client.pem"), Path.of("/client.key"), Path.of("/ca.pem")));
    }

    @DisplayName("a client certificate without its key is rejected")
    @Test
    void clientCertWithoutKeyRejected() {
        assertThatThrownBy(() -> fromEnv(Map.of(
                        OtlpEnv.ENDPOINT, "https://collector:4317",
                        OtlpEnv.CLIENT_CERTIFICATE, "/client.pem")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("cert vars on an http endpoint are ignored (plaintext wins)")
    @Test
    void certVarsIgnoredOnHttp() {
        var cfg = fromEnv(Map.of(
                OtlpEnv.ENDPOINT, "http://collector:4317",
                OtlpEnv.CERTIFICATE, "/ca.pem"));

        assertThat(cfg.tls()).isEqualTo(Tls.disabled());
    }

    @DisplayName("headers parse in order and percent-decode values, leaving '+' literal")
    @Test
    void headers() {
        var cfg = fromEnv(Map.of(OtlpEnv.HEADERS, " authorization=Bearer%20xyz , x-tenant=a+b "));

        assertThat(cfg.headers())
                .containsExactly(Map.entry("authorization", "Bearer xyz"), Map.entry("x-tenant", "a+b"));
    }

    @DisplayName("env headers merge onto headers already set, winning per key")
    @Test
    void headersMerge() {
        var merged = ClientConfig.builder()
                .header("authorization", "Bearer keep-me")
                .header("x-tenant", "old")
                .fromEnvironment(Map.of(OtlpEnv.HEADERS, "x-tenant=new,x-trace=on")::get)
                .build();

        assertThat(merged.headers())
                .containsEntry("authorization", "Bearer keep-me") // unrelated key preserved
                .containsEntry("x-tenant", "new") // env wins per key
                .containsEntry("x-trace", "on");
    }

    @DisplayName("a header entry without '=' or with an empty key is rejected")
    @Test
    void malformedHeaders() {
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.HEADERS, "authorization")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.HEADERS, "=value")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("timeout parses integer milliseconds and rejects non-positive or non-numeric values")
    @Test
    void timeout() {
        assertThat(fromEnv(Map.of(OtlpEnv.TIMEOUT, "5000")).timeout()).isEqualTo(Duration.ofMillis(5000));
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.TIMEOUT, "0")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.TIMEOUT, "-1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.TIMEOUT, "abc")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("compression parses gzip/none case-insensitively and rejects others")
    @Test
    void compression() {
        assertThat(fromEnv(Map.of(OtlpEnv.COMPRESSION, "gzip")).compression()).isEqualTo(Compression.GZIP);
        assertThat(fromEnv(Map.of(OtlpEnv.COMPRESSION, "GZIP")).compression()).isEqualTo(Compression.GZIP);
        assertThat(fromEnv(Map.of(OtlpEnv.COMPRESSION, "none")).compression()).isEqualTo(Compression.NONE);
        assertThatThrownBy(() -> fromEnv(Map.of(OtlpEnv.COMPRESSION, "snappy")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("an empty environment leaves every built-in default intact")
    @Test
    void emptyEnvIsDefaults() {
        assertThat(fromEnv(Map.of())).isEqualTo(ClientConfig.builder().build());
    }

    @DisplayName("partial environment only overrides the variables present")
    @Test
    void partialEnv() {
        var cfg = fromEnv(Map.of(OtlpEnv.TIMEOUT, "2500"));

        assertThat(cfg.timeout()).isEqualTo(Duration.ofMillis(2500));
        assertThat(cfg.host()).isEqualTo("localhost");
        assertThat(cfg.port()).isEqualTo(4317);
        assertThat(cfg.tls()).isEqualTo(Tls.disabled());
        assertThat(cfg.compression()).isEqualTo(Compression.NONE);
    }

    @DisplayName("explicit setters after fromEnvironment() override the environment")
    @Test
    void builderWinsAfterEnv() {
        var cfg = ClientConfig.builder()
                .fromEnvironment(Map.of(OtlpEnv.ENDPOINT, "http://collector:4317")::get)
                .endpoint("override", 9999)
                .build();

        assertThat(cfg.host()).isEqualTo("override");
        assertThat(cfg.port()).isEqualTo(9999);
    }

    @DisplayName("fromEnvironment() overrides setters called before it")
    @Test
    void envWinsOverEarlierSetters() {
        var cfg = ClientConfig.builder()
                .endpoint("pre", 1)
                .fromEnvironment(Map.of(OtlpEnv.ENDPOINT, "http://collector:4317")::get)
                .build();

        assertThat(cfg.host()).isEqualTo("collector");
        assertThat(cfg.port()).isEqualTo(4317);
    }
}
