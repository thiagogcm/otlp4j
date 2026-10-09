package dev.nthings.otlp4j.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.testing.conformance.ConformanceSignal;
import dev.nthings.otlp4j.testing.conformance.GrpcWireClient;
import dev.nthings.otlp4j.testing.conformance.HttpWireClient;
import dev.nthings.otlp4j.testing.conformance.TlsMode;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import dev.nthings.otlp4j.transport.OtlpReceiver;
import dev.nthings.otlp4j.transport.ClientAuth;
import io.grpc.Status;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// The enforced mutual-TLS conformance case. It boots the Armeria OTLP/gRPC and OTLP/HTTP receivers
/// directly through [OtlpReceiver.Builder] with [ClientAuth#REQUIRE] and the test CA as the
/// trust anchor, then drives them over the real wire: a client presenting the test client certificate
/// is accepted and a certless client is rejected at the TLS handshake.
///
/// This lives in the conformance module rather than in the reusable suites in otlp4j-testing, because
/// it names the Armeria factory and the new [ClientAuth] knob directly. The shared black-box suites,
/// which must pass against both protocols, never learn about transport-specific authentication setup; the
/// pinned mutual-TLS-accepted-but-not-required case in those suites stays green on both transports
/// under the default [ClientAuth#NONE].
@Timeout(60)
@DisplayName("Armeria enforced mutual TLS (conformance)")
final class ArmeriaMutualTlsRequiredTest {

    private static final List<OtlpReceiver> receivers = new CopyOnWriteArrayList<>();

    /// Stops the receivers of every case together: one graceful stop takes about three seconds, so
    /// stopping after each case would cost that per case.
    @AfterAll
    static void teardown() {
        var stops = receivers.stream()
                .map(receiver -> receiver.shutdown(Duration.ofSeconds(30)).toCompletableFuture())
                .toList();
        for (var stop : stops) {
            try {
                stop.get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    @DisplayName("gRPC REQUIRE: a client presenting its certificate is accepted")
    @Test
    void grpcClientCertificateAccepted() {
        var target = "127.0.0.1:" + requireReceiver(OtlpReceiver.grpcBuilder()).port();
        try (var client = new GrpcWireClient(target, TlsMode.MUTUAL, TlsTestMaterial.caPem())) {
            assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.OK);
        }
    }

    @DisplayName("gRPC REQUIRE: a certless client is rejected at the handshake")
    @Test
    void grpcCertlessClientRejected() {
        var target = "127.0.0.1:" + requireReceiver(OtlpReceiver.grpcBuilder()).port();
        try (var client = new GrpcWireClient(target, TlsMode.SERVER_AUTH, TlsTestMaterial.caPem())) {
            // A certless client cannot complete the required-client-auth handshake, so the RPC never
            // reaches an accepting server: gRPC surfaces the handshake failure as a non-OK status.
            assertThat(client.export(ConformanceSignal.TRACES).grpcCode().orElse(null))
                    .isNotEqualTo(Status.Code.OK);
        }
    }

    @DisplayName("HTTP REQUIRE: a client presenting its certificate is accepted")
    @Test
    void httpClientCertificateAccepted() {
        var baseUri = URI.create("https://127.0.0.1:" + requireReceiver(OtlpReceiver.httpBuilder()).port());
        try (var client = new HttpWireClient(baseUri, TlsMode.MUTUAL, TlsTestMaterial.caPem())) {
            assertThat(client.export(ConformanceSignal.TRACES).httpStatus()).hasValue(200);
        }
    }

    @DisplayName("HTTP REQUIRE: a certless client is rejected at the handshake")
    @Test
    void httpCertlessClientRejected() {
        var baseUri = URI.create("https://127.0.0.1:" + requireReceiver(OtlpReceiver.httpBuilder()).port());
        try (var client = new HttpWireClient(baseUri, TlsMode.SERVER_AUTH, TlsTestMaterial.caPem())) {
            assertThatThrownBy(() -> client.export(ConformanceSignal.TRACES))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    /// Boots a started receiver from `builder` with enforced mutual TLS: the test server certificate
    /// and key, the test CA as the client-auth trust anchor, [ClientAuth#REQUIRE], an accepting route
    /// for every signal, and an ephemeral port.
    private OtlpReceiver requireReceiver(OtlpReceiver.Builder builder) {
        var receiver = builder
                .tls(Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(),
                        TlsTestMaterial.caPem()))
                .clientAuth(ClientAuth.REQUIRE)
                .route(SignalDescriptor.TRACES, accepting())
                .route(SignalDescriptor.METRICS, accepting())
                .route(SignalDescriptor.LOGS, accepting())
                .route(SignalDescriptor.PROFILES, accepting())
                .ephemeralPort()
                .build()
                .start();
        receivers.add(receiver);
        return receiver;
    }

    /// A minimal accepting consumer; the enforced-mTLS assertions turn only on the handshake, so the
    /// consumer body never has to do more than accept.
    private static <T> SignalConsumer<T> accepting() {
        var provenance = new Provenance(
                ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);
        return new SignalConsumer<>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
                return CompletableFuture.completedFuture(DeliveryResult.accepted(provenance));
            }
        };
    }
}
