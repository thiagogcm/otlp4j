package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
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

    private static final PayloadDispatch NO_OP = new PayloadDispatch() {
        @Override
        public CompletionStage<DeliveryResult> dispatch(ConsumeContext context, Payload<?> payload) {
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }
    };

    private static final Set<Protocol> GRPC = EnumSet.of(Protocol.GRPC);
    private static final Set<Protocol> HTTP = EnumSet.of(Protocol.HTTP);
    private static final Set<Protocol> BOTH = EnumSet.of(Protocol.GRPC, Protocol.HTTP);

    private static OtlpReceiverConfig ephemeral() {
        return OtlpReceiverConfig.builder(4317).port(0).build();
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
        var server = new ArmeriaOtlpServer(ephemeral(), NO_OP, GRPC);
        var shutdown = server.shutdown(Duration.ZERO);
        shutdown.toCompletableFuture().join();
        assertThat(server.shutdown(Duration.ofSeconds(1)) == shutdown).isTrue();
        assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
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
            server.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
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
        var config = OtlpReceiverConfig.builder(4317).bindHost("127.0.0.1").port(0).build();
        var server = new ArmeriaOtlpServer(config, NO_OP, GRPC);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
        }
    }

    @DisplayName("the hardening knobs are accepted and the server still starts")
    @Test
    void hardeningKnobsStart() throws Exception {
        var config = OtlpReceiverConfig.builder(4317)
                .port(0)
                .maxInboundMessageSizeBytes(1024 * 1024)
                .build();
        var server = new ArmeriaOtlpServer(config, NO_OP, BOTH);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
        }
    }

    @DisplayName("SystemTrust TLS is rejected for a server")
    @Test
    void systemTrustRejected() {
        var server = new ArmeriaOtlpServer(
                OtlpReceiverConfig.builder(4317).port(0).tls(Tls.systemTrust()).build(), NO_OP, GRPC);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SystemTrust");
    }

    @DisplayName("in-memory server TLS missing the key is rejected")
    @Test
    void incompleteInlineRejected() {
        var server = new ArmeriaOtlpServer(
                OtlpReceiverConfig.builder(4317).port(0)
                        .tls(Tls.custom(new byte[] {1, 2, 3}, (byte[]) null, null)).build(),
                NO_OP, HTTP);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
    }

    @DisplayName("Custom server TLS missing the key is rejected")
    @Test
    void incompleteCustomRejected() {
        var server = new ArmeriaOtlpServer(
                OtlpReceiverConfig.builder(4317).port(0)
                        .tls(Tls.custom(Path.of("server.crt"), null, null)).build(),
                NO_OP, GRPC);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
    }

}
