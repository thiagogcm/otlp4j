package dev.nthings.otlp4j.transport.grpc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Lifecycle contract for [GrpcOtlpServer] in isolation.
@Timeout(30)
@DisplayName("GrpcOtlpServer lifecycle")
class GrpcOtlpServerLifecycleTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    private static final Dispatchers NO_OP = new Dispatchers() {
        @Override
        public CompletionStage<DeliveryResult> dispatch(TelemetryPayload payload) {
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }
    };

    private static ServerConfig ephemeral() {
        return ServerConfig.builder().setPort(0).build();
    }

    @DisplayName("shutdown before start is a safe no-op")
    @Test
    void shutdownBeforeStartIsASafeNoOp() {
        assertThatCode(() -> new GrpcOtlpServer(ephemeral(), NO_OP)
                .shutdown(Duration.ofSeconds(1)).toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("shutdownNow before start is a safe no-op")
    @Test
    void shutdownNowBeforeStartIsASafeNoOp() {
        assertThatCode(() -> new GrpcOtlpServer(ephemeral(), NO_OP)
                .shutdownNow().toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("port is zero before the server starts")
    @Test
    void portBeforeStartIsZero() {
        assertThat(new GrpcOtlpServer(ephemeral(), NO_OP).port()).isZero();
    }

    @DisplayName("starting an already-started server throws IllegalStateException")
    @Test
    void doubleStartThrowsIllegalState() throws Exception {
        var server = new GrpcOtlpServer(ephemeral(), NO_OP);
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

    @DisplayName("a loopback-only bindHost binds successfully and reports a positive port")
    @Test
    void loopbackBindHostStarts() throws Exception {
        var config = ServerConfig.builder().setBindHost("127.0.0.1").setPort(0).build();
        var server = new GrpcOtlpServer(config, NO_OP);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("graceful shutdown(timeout) of a started server completes")
    @Test
    void gracefulShutdownOfStartedServer() throws Exception {
        var server = new GrpcOtlpServer(ephemeral(), NO_OP);
        server.start();
        assertThat(server.port()).isPositive();
        // The graceful path (server.shutdown() then awaitTermination success), not shutdownNow().
        server.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
    }

    @DisplayName("SystemTrust TLS is rejected for a server")
    @Test
    void systemTrustTlsRejectedForServer() {
        var server = new GrpcOtlpServer(
                ServerConfig.builder().setPort(0).setTls(Tls.systemTrust()).build(), NO_OP);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SystemTrust");
    }

    @DisplayName("Custom server TLS missing the key is rejected")
    @Test
    void incompleteCustomTlsRejectedForServer() {
        var server = new GrpcOtlpServer(
                ServerConfig.builder().setPort(0)
                        .setTls(Tls.custom(Path.of("server.crt"), null, null)).build(),
                NO_OP);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
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
        // The gRPC-only per-connection concurrency cap is now a constructor arg, not a ServerConfig field.
        var server = new GrpcOtlpServer(config, NO_OP, 16);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdownNow().toCompletableFuture().join();
        }
    }
}
