package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.server.Server;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/// Construction, TLS-mapping, and teardown contract for [ArmeriaOtlpClient] and [ArmeriaTls] in
/// isolation. Constructing a client builds its stubs or web client and its TLS client factory without
/// connecting, so every [Tls] variant's client-factory branch is exercised cheaply.
@Timeout(60)
@DisplayName("ArmeriaOtlpClient and ArmeriaTls")
class ArmeriaOtlpClientLifecycleTest {

    private static OtlpExporterConfig config(Tls tls) {
        return OtlpExporterConfig.builder(4317).setEndpoint("localhost", 4317).setTls(tls).build();
    }

    private static void shutdown(ArmeriaOtlpClient client) {
        client.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
    }

    @DisplayName("a plaintext gRPC client builds and drains")
    @Test
    void plaintextGrpcDrains() {
        var client = new ArmeriaOtlpClient(config(Tls.disabled()), Protocol.GRPC);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("a plaintext HTTP client closes through try-with-resources")
    @Test
    void plaintextHttpCloses() {
        var client = new ArmeriaOtlpClient(config(Tls.disabled()), Protocol.HTTP);
        assertThatCode(client::close).doesNotThrowAnyException();
    }

    @DisplayName("forceFlush is a no-op that completes")
    @Test
    void forceFlushCompletes() {
        var client = new ArmeriaOtlpClient(config(Tls.disabled()), Protocol.GRPC);
        try {
            client.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        } finally {
            shutdown(client);
        }
    }

    @DisplayName("a system-trust HTTP client builds its default factory and drains")
    @Test
    void systemTrustHttpDrains() {
        var client = new ArmeriaOtlpClient(config(Tls.systemTrust()), Protocol.HTTP);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("an in-memory trust-only client builds a trust factory and drains")
    @Test
    void inlineTrustOnlyDrains() {
        var client = new ArmeriaOtlpClient(config(Tls.trust(TlsTestMaterial.caPem())), Protocol.GRPC);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("an in-memory mutual-TLS client builds a key-and-trust factory and drains")
    @Test
    void inlineMutualDrains() {
        var tls = Tls.custom(
                TlsTestMaterial.clientCertPem(), TlsTestMaterial.clientKeyPem(), TlsTestMaterial.caPem());
        var client = new ArmeriaOtlpClient(config(tls), Protocol.HTTP);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("a file-based mutual-TLS client builds a key-and-trust factory and drains")
    @Test
    void customMutualDrains(@TempDir Path dir) throws Exception {
        var cert = write(dir, "client-cert.pem", TlsTestMaterial.clientCertPem());
        var key = write(dir, "client-key.pem", TlsTestMaterial.clientKeyPem());
        var ca = write(dir, "ca.pem", TlsTestMaterial.caPem());
        var client = new ArmeriaOtlpClient(config(Tls.custom(cert, key, ca)), Protocol.GRPC);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("an in-memory mutual-TLS client without extra trust drains")
    @Test
    void inlineMutualNoTrustDrains() {
        var tls = Tls.custom(TlsTestMaterial.clientCertPem(), TlsTestMaterial.clientKeyPem(), null);
        var client = new ArmeriaOtlpClient(config(tls), Protocol.GRPC);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("a file-based mutual-TLS client without extra trust drains")
    @Test
    void customMutualNoTrustDrains(@TempDir Path dir) throws Exception {
        var cert = write(dir, "client-cert.pem", TlsTestMaterial.clientCertPem());
        var key = write(dir, "client-key.pem", TlsTestMaterial.clientKeyPem());
        var client = new ArmeriaOtlpClient(config(Tls.custom(cert, key, null)), Protocol.HTTP);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("an SSLContext client honours the trust manager only and drains")
    @Test
    void sslContextDrains() {
        var tls = Tls.sslContext(sslContext(), trustManager());
        var client = new ArmeriaOtlpClient(config(tls), Protocol.HTTP);
        assertThatCode(() -> shutdown(client)).doesNotThrowAnyException();
    }

    @DisplayName("incomplete client mutual-TLS material is rejected at construction")
    @Test
    void incompleteMutualRejected() {
        var tls = Tls.custom(TlsTestMaterial.clientCertPem(), (byte[]) null, TlsTestMaterial.caPem());
        assertThatThrownBy(() -> new ArmeriaOtlpClient(config(tls), Protocol.GRPC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and key");
    }

    @DisplayName("ArmeriaTls.applyServer rejects Tls.Disabled directly")
    @Test
    void applyServerRejectsDisabled() {
        assertThatThrownBy(() -> ArmeriaTls.applyServer(
                        Server.builder(), Tls.disabled(),
                        dev.nthings.otlp4j.transport.ClientAuth.NONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Disabled");
    }

    private static javax.net.ssl.SSLContext sslContext() {
        try {
            return javax.net.ssl.SSLContext.getDefault();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static javax.net.ssl.X509TrustManager trustManager() {
        try {
            var tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                    javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((java.security.KeyStore) null);
            return (javax.net.ssl.X509TrustManager) tmf.getTrustManagers()[0];
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Path write(Path dir, String name, byte[] bytes) throws Exception {
        var path = dir.resolve(name);
        Files.write(path, bytes);
        return path;
    }
}
