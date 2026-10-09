package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/// Enforced mutual TLS through the transport-specific [ClientAuth] builder knob. Under
/// [ClientAuth#REQUIRE] a certless client is rejected at
/// the TLS handshake and a client presenting the test certificate succeeds; under [ClientAuth#OPTIONAL]
/// a certless client still succeeds. The neutral config records are untouched: the mode is an
/// Armeria-builder knob only.
@Timeout(60)
@DisplayName("Armeria enforced mutual TLS")
class ArmeriaMutualTlsTest {

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

    @DisplayName("gRPC REQUIRE: a client presenting its certificate is accepted")
    @Test
    void grpcRequireAcceptsClientCert() {
        var receiver = requireReceiver(OtlpReceiver.grpcBuilder());
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.custom(TlsTestMaterial.clientCertPem(), TlsTestMaterial.clientKeyPem(),
                        TlsTestMaterial.caPem()))
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);

        assertThat(exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("gRPC REQUIRE: a certless client is rejected at the handshake")
    @Test
    void grpcRequireRejectsCertlessClient() {
        var receiver = requireReceiver(OtlpReceiver.grpcBuilder());
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.trust(TlsTestMaterial.caPem()))
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("HTTP REQUIRE: a client presenting its certificate is accepted")
    @Test
    void httpRequireAcceptsClientCert() {
        var receiver = requireReceiver(OtlpReceiver.httpBuilder());
        var exporter = OtlpExporter.httpBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.custom(TlsTestMaterial.clientCertPem(), TlsTestMaterial.clientKeyPem(),
                        TlsTestMaterial.caPem()))
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);

        assertThat(exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("HTTP REQUIRE: a certless client is rejected at the handshake")
    @Test
    void httpRequireRejectsCertlessClient() {
        var receiver = requireReceiver(OtlpReceiver.httpBuilder());
        var exporter = OtlpExporter.httpBuilder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.trust(TlsTestMaterial.caPem()))
                .timeout(Duration.ofSeconds(5))
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("OPTIONAL: a certless client still succeeds")
    @Test
    void optionalAcceptsCertlessClient() {
        var receiver = OtlpReceiver.grpcBuilder()
                .tls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(),
                        TlsTestMaterial.caPem()))
                .clientAuth(ClientAuth.OPTIONAL)
                .route(TRACES, Consumers.accepting())
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

        assertThat(exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("REQUIRE without a trust anchor fails fast at start")
    @Test
    void requireWithoutTrustAnchorFails() {
        var receiver = OtlpReceiver.grpcBuilder()
                .tls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null))
                .clientAuth(ClientAuth.REQUIRE)
                .route(TRACES, Consumers.accepting())
                .ephemeralPort()
                .build();
        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trust anchor");
    }

    private OtlpReceiver requireReceiver(OtlpReceiver.Builder builder) {
        var receiver = builder
                .tls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(),
                        TlsTestMaterial.caPem()))
                .clientAuth(ClientAuth.REQUIRE)
                .route(TRACES, Consumers.accepting())
                .ephemeralPort()
                .build()
                .start();
        cleanup.receiver(receiver);
        return receiver;
    }
}
