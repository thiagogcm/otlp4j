package dev.nthings.otlp4j.transport.otlp;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/// Server and client TLS over Armeria, exercised with the checked-in test PEM material: in-memory
/// (Inline) material over gRPC, file-based (Custom) material over HTTP, and an untrusted-client
/// handshake failure.
@Timeout(60)
@DisplayName("Armeria transport TLS")
class ArmeriaTlsTest {

    private final List<ServerReceiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    @DisplayName("in-memory server TLS with a trusting client round-trips over gRPC")
    @Test
    void inlineTlsRoundTripsGrpc() {
        var received = new AtomicReference<TracesData>();
        var receiver = OtlpArmeriaReceiver.grpcBuilder()
                .setTls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null))
                .route(TRACES, Consumers.record(TRACES, received))
                .ephemeralPort()
                .build()
                .start();
        receivers.add(receiver);

        var exporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(TlsTestMaterial.caPem()))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
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
        var receiver = OtlpArmeriaReceiver.httpBuilder()
                .setTls(Tls.custom(certFile, keyFile, null))
                .route(TRACES, Consumers.record(TRACES, received))
                .ephemeralPort()
                .build()
                .start();
        receivers.add(receiver);

        var exporter = OtlpArmeriaExporter.httpBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(caFile))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("an untrusted client fails the TLS handshake")
    @Test
    void untrustedClientFailsHandshake() {
        var receiver = OtlpArmeriaReceiver.grpcBuilder()
                .setTls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null))
                .route(TRACES, Consumers.accepting())
                .ephemeralPort()
                .build()
                .start();
        receivers.add(receiver);

        // System-trust client does not trust the self-signed test CA, so the handshake fails.
        var exporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.systemTrust())
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

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
