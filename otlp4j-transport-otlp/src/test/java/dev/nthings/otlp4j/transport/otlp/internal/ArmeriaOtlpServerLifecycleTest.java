package dev.nthings.otlp4j.transport.otlp.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.Dispatchers;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Lifecycle contract for [ArmeriaOtlpServer] in isolation, across the gRPC-only, HTTP-only, and
/// dual-protocol configurations.
@Timeout(60)
@DisplayName("ArmeriaOtlpServer lifecycle")
class ArmeriaOtlpServerLifecycleTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    private static final Dispatchers NO_OP = new Dispatchers() {
        @Override
        public CompletionStage<DeliveryResult> dispatch(TelemetryPayload payload) {
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }
    };

    private static final Set<Protocol> GRPC = EnumSet.of(Protocol.GRPC);
    private static final Set<Protocol> HTTP = EnumSet.of(Protocol.HTTP);
    private static final Set<Protocol> BOTH = EnumSet.of(Protocol.GRPC, Protocol.HTTP);

    private static ServerConfig ephemeral() {
        return ServerConfig.builder().setPort(0).build();
    }

    @DisplayName("an empty protocol set is rejected")
    @Test
    void emptyProtocolsRejected() {
        assertThatThrownBy(() -> new ArmeriaOtlpServer(ephemeral(), NO_OP, Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one protocol");
    }

    @DisplayName("shutdown before start is a safe no-op")
    @Test
    void shutdownBeforeStartIsANoOp() {
        assertThatCode(() -> new ArmeriaOtlpServer(ephemeral(), NO_OP, GRPC)
                .shutdown(Duration.ofSeconds(1)).toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("shutdownNow before start is a safe no-op")
    @Test
    void shutdownNowBeforeStartIsANoOp() {
        assertThatCode(() -> new ArmeriaOtlpServer(ephemeral(), NO_OP, HTTP)
                .shutdownNow().toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("port is zero before start")
    @Test
    void portBeforeStartIsZero() {
        assertThat(new ArmeriaOtlpServer(ephemeral(), NO_OP, GRPC).port()).isZero();
    }

    @DisplayName("double start throws IllegalStateException")
    @Test
    void doubleStartThrows() throws Exception {
        var server = new ArmeriaOtlpServer(ephemeral(), NO_OP, GRPC);
        server.start();
        try {
            assertThat(server.port()).isPositive();
            assertThatThrownBy(server::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already started");
        } finally {
            server.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("a dual-protocol server binds one positive port")
    @Test
    void dualProtocolBindsOnePort() throws Exception {
        var server = new ArmeriaOtlpServer(ephemeral(), NO_OP, BOTH);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
        }
    }

    @DisplayName("an HTTP-only server starts and gracefully shuts down")
    @Test
    void httpOnlyStartsAndDrains() throws Exception {
        var server = new ArmeriaOtlpServer(ephemeral(), NO_OP, HTTP);
        server.start();
        assertThat(server.port()).isPositive();
        server.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
    }

    @DisplayName("a loopback-only bindHost binds successfully")
    @Test
    void loopbackBindStarts() throws Exception {
        var config = ServerConfig.builder().setBindHost("127.0.0.1").setPort(0).build();
        var server = new ArmeriaOtlpServer(config, NO_OP, GRPC);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("the hardening knobs are accepted and the server still starts")
    @Test
    void hardeningKnobsStart() throws Exception {
        var config = ServerConfig.builder()
                .setPort(0)
                .setMaxInboundMessageSizeBytes(1024 * 1024)
                .setHandshakeTimeout(Duration.ofSeconds(5))
                .setServerExecutor(Runnable::run)
                .build();
        var server = new ArmeriaOtlpServer(config, NO_OP, BOTH);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("SystemTrust TLS is rejected for a server")
    @Test
    void systemTrustRejected() {
        var server = new ArmeriaOtlpServer(
                ServerConfig.builder().setPort(0).setTls(Tls.systemTrust()).build(), NO_OP, GRPC);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SystemTrust");
    }

    @DisplayName("SslContext TLS is rejected for a server")
    @Test
    void sslContextRejected() throws Exception {
        var ctx = javax.net.ssl.SSLContext.getDefault();
        var trust = (javax.net.ssl.X509TrustManager) trustManager();
        var server = new ArmeriaOtlpServer(
                ServerConfig.builder().setPort(0).setTls(Tls.sslContext(ctx, trust)).build(), NO_OP, HTTP);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SslContext");
    }

    @DisplayName("in-memory server TLS missing the key is rejected")
    @Test
    void incompleteInlineRejected() {
        var server = new ArmeriaOtlpServer(
                ServerConfig.builder().setPort(0)
                        .setTls(Tls.custom(new byte[] {1, 2, 3}, (byte[]) null, null)).build(),
                NO_OP, HTTP);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
    }

    @DisplayName("Custom server TLS missing the key is rejected")
    @Test
    void incompleteCustomRejected() {
        var server = new ArmeriaOtlpServer(
                ServerConfig.builder().setPort(0)
                        .setTls(Tls.custom(Path.of("server.crt"), null, null)).build(),
                NO_OP, GRPC);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
    }

    private static javax.net.ssl.TrustManager trustManager() throws Exception {
        var tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((java.security.KeyStore) null);
        return tmf.getTrustManagers()[0];
    }
}
