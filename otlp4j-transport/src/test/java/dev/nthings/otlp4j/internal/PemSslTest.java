package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box tests for [PemSsl]: building [javax.net.ssl.SSLContext]s from PEM material, covering the
/// server key manager, the client trust-only and mutual-TLS paths, and the failure path.
@DisplayName("PEM SSLContext loading")
class PemSslTest {

    private static final Path CERT = resource("/tls/server.crt");
    private static final Path KEY = resource("/tls/server.key");

    @DisplayName("Builds a server context from a certificate and key")
    @Test
    void buildsServerContext() {
        var context = PemSsl.serverContext(CERT, KEY);
        assertThat(context).isNotNull();
        assertThat(context.getProtocol()).isEqualTo("TLS");
    }

    @DisplayName("Builds a client context that trusts a custom certificate")
    @Test
    void buildsClientTrustOnlyContext() {
        assertThat(PemSsl.clientContext(null, null, CERT)).isNotNull();
    }

    @DisplayName("Builds a client context presenting a certificate for mutual TLS")
    @Test
    void buildsClientMutualTlsContext() {
        assertThat(PemSsl.clientContext(CERT, KEY, CERT)).isNotNull();
    }

    @DisplayName("A missing certificate file fails fast")
    @Test
    void missingMaterialThrows() {
        var missing = Path.of("does-not-exist.crt");
        assertThatThrownBy(() -> PemSsl.serverContext(missing, KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("server TLS material");
        assertThatThrownBy(() -> PemSsl.clientContext(null, null, missing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("client TLS material");
    }

    private static Path resource(String name) {
        try {
            return Path.of(PemSslTest.class.getResource(name).toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
