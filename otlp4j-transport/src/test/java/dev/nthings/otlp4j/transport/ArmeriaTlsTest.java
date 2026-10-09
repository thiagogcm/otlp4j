package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/// Server and client TLS over Armeria, exercised with the checked-in test PEM material: in-memory
/// (Inline) material over gRPC, file-based (Custom) material over HTTP, and an untrusted-client
/// handshake failure.
@Timeout(60)
@DisplayName("Armeria transport TLS")
class ArmeriaTlsTest {

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

    @DisplayName("in-memory TLS material is defensively copied")
    @Test
    void inlineMaterialIsDefensivelyCopied() {
        var cert = new byte[] {1};
        var key = new byte[] {2};
        var trust = new byte[] {3};
        var tls = (Tls.Inline) Tls.custom(cert, key, trust);

        cert[0] = 9;
        key[0] = 9;
        trust[0] = 9;
        assertThat(tls.cert()).containsExactly(1);
        assertThat(tls.key()).containsExactly(2);
        assertThat(tls.trust()).containsExactly(3);

        tls.cert()[0] = 8;
        tls.key()[0] = 8;
        tls.trust()[0] = 8;
        assertThat(tls.cert()).containsExactly(1);
        assertThat(tls.key()).containsExactly(2);
        assertThat(tls.trust()).containsExactly(3);

        assertThat(tls)
                .isEqualTo(Tls.custom(new byte[] {1}, new byte[] {2}, new byte[] {3}))
                .hasSameHashCodeAs(Tls.custom(new byte[] {1}, new byte[] {2}, new byte[] {3}));
    }

    @DisplayName("in-memory server TLS with a trusting client round-trips over gRPC")
    @Test
    void inlineTlsRoundTripsGrpc() {
        var received = new AtomicReference<TracesData>();
        var receiver = OtlpReceiver.grpcBuilder()
                .tls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null))
                .route(TRACES, Consumers.record(TRACES, received))
                .ephemeralPort()
                .build()
                .start();
        cleanup.receiver(receiver);

        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.trust(TlsTestMaterial.caPem()))
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("file-based server TLS with a trusting client round-trips over HTTP")
    @Test
    void customTlsRoundTripsHttp(@TempDir Path dir) throws Exception {
        var certFile = write(dir, "server-cert.pem", TlsTestMaterial.serverCertPem());
        var keyFile = write(dir, "server-key.pem", TlsTestMaterial.serverKeyPem());
        var caFile = write(dir, "ca.pem", TlsTestMaterial.caPem());

        var received = new AtomicReference<TracesData>();
        var receiver = OtlpReceiver.httpBuilder()
                .tls(Tls.custom(certFile, keyFile, null))
                .route(TRACES, Consumers.record(TRACES, received))
                .ephemeralPort()
                .build()
                .start();
        cleanup.receiver(receiver);

        var exporter = OtlpExporter.httpBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.trust(caFile))
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("an untrusted client fails the TLS handshake")
    @Test
    void untrustedClientFailsHandshake() {
        var receiver = OtlpReceiver.grpcBuilder()
                .tls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null))
                .route(TRACES, Consumers.accepting())
                .ephemeralPort()
                .build()
                .start();
        cleanup.receiver(receiver);

        // System-trust client does not trust the self-signed test CA, so the handshake fails.
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.systemTrust())
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    private static Path write(Path dir, String name, byte[] bytes) throws Exception {
        var path = dir.resolve(name);
        Files.write(path, bytes);
        return path;
    }
}
