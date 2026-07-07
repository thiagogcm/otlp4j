package dev.nthings.otlp4j.transport.otlp;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import dev.nthings.otlp4j.transport.otlp.OtlpArmeriaReceiver.ClientAuth;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Enforced mutual TLS, the new capability the transport-specific [ClientAuth] builder knob adds and
/// the legacy transports could not express. Under [ClientAuth#REQUIRE] a certless client is rejected at
/// the TLS handshake and a client presenting the test certificate succeeds; under [ClientAuth#OPTIONAL]
/// a certless client still succeeds. The neutral config records are untouched: the mode is an
/// Armeria-builder knob only.
@Timeout(60)
@DisplayName("Armeria enforced mutual TLS")
class ArmeriaMutualTlsTest {

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

    @DisplayName("gRPC REQUIRE: a client presenting its certificate is accepted")
    @Test
    void grpcRequireAcceptsClientCert() {
        var receiver = requireReceiver(OtlpArmeriaReceiver.grpcBuilder());
        var exporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.custom(TlsTestMaterial.clientCertPem(), TlsTestMaterial.clientKeyPem(),
                        TlsTestMaterial.caPem()))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

        assertThat(exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("gRPC REQUIRE: a certless client is rejected at the handshake")
    @Test
    void grpcRequireRejectsCertlessClient() {
        var receiver = requireReceiver(OtlpArmeriaReceiver.grpcBuilder());
        var exporter = OtlpArmeriaExporter.grpcBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(TlsTestMaterial.caPem()))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("HTTP REQUIRE: a client presenting its certificate is accepted")
    @Test
    void httpRequireAcceptsClientCert() {
        var receiver = requireReceiver(OtlpArmeriaReceiver.httpBuilder());
        var exporter = OtlpArmeriaExporter.httpBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.custom(TlsTestMaterial.clientCertPem(), TlsTestMaterial.clientKeyPem(),
                        TlsTestMaterial.caPem()))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

        assertThat(exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("HTTP REQUIRE: a certless client is rejected at the handshake")
    @Test
    void httpRequireRejectsCertlessClient() {
        var receiver = requireReceiver(OtlpArmeriaReceiver.httpBuilder());
        var exporter = OtlpArmeriaExporter.httpBuilder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(TlsTestMaterial.caPem()))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("OPTIONAL: a certless client still succeeds")
    @Test
    void optionalAcceptsCertlessClient() {
        var receiver = OtlpArmeriaReceiver.grpcBuilder()
                .setTls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(),
                        TlsTestMaterial.caPem()))
                .setClientAuth(ClientAuth.OPTIONAL)
                .route(TRACES, Consumers.accepting())
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

        assertThat(exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("REQUIRE without a trust anchor fails fast at start")
    @Test
    void requireWithoutTrustAnchorFails() {
        var receiver = OtlpArmeriaReceiver.grpcBuilder()
                .setTls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null))
                .setClientAuth(ClientAuth.REQUIRE)
                .route(TRACES, Consumers.accepting())
                .ephemeralPort()
                .build();
        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trust anchor");
    }

    private ServerReceiver requireReceiver(OtlpArmeriaReceiver.Builder builder) {
        var receiver = builder
                .setTls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(),
                        TlsTestMaterial.caPem()))
                .setClientAuth(ClientAuth.REQUIRE)
                .route(TRACES, Consumers.accepting())
                .ephemeralPort()
                .build()
                .start();
        receivers.add(receiver);
        return receiver;
    }
}
