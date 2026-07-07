package dev.nthings.otlp4j.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.transport.common.AbstractReceiverBuilder;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpServer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Pins the gRPC-only `maxConcurrentCallsPerConnection` knob: it is a `GrpcOtlpServer` constructor
/// arg threaded from the gRPC builder, not a shared [ServerConfig] field (HTTP has no equivalent).
@Timeout(30)
@DisplayName("gRPC-only maxConcurrentCallsPerConnection knob")
class GrpcMaxConcurrentTest {

    private static final Dispatchers NO_OP = new Dispatchers() {
        @Override
        public CompletionStage<DeliveryResult> dispatch(TelemetryPayload payload) {
            return CompletableFuture.completedStage(DeliveryResult.accepted(Consumers.PROV));
        }
    };

    @DisplayName("a positive cap is applied to Netty and the server still binds")
    @Test
    void positiveCapAppliesAndBinds() throws Exception {
        var server = new GrpcOtlpServer(ServerConfig.builder().setPort(0).build(), NO_OP, 8);
        server.start();
        try {
            assertThat(server.port()).isPositive();
        } finally {
            server.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("the gRPC receiver builder threads the cap into a server that binds")
    @Test
    void receiverBuilderRoutesTheCap() throws Exception {
        var receiver = OtlpGrpcReceiver.builder()
                .ephemeralPort()
                .setMaxConcurrentCallsPerConnection(4)
                .route(TRACES, Consumers.accepting())
                .build();
        receiver.start();
        try {
            assertThat(receiver.port()).isPositive();
        } finally {
            receiver.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("the gRPC builder rejects a negative cap")
    @Test
    void rejectsNegativeCap() {
        assertThatThrownBy(() -> OtlpGrpcReceiver.builder().setMaxConcurrentCallsPerConnection(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrentCallsPerConnection");
    }

    @DisplayName("the knob is gRPC-only: the shared receiver base exposes no such setter")
    @Test
    void sharedBaseHasNoSuchKnob() {
        var present = Stream.of(AbstractReceiverBuilder.class.getMethods())
                .anyMatch(m -> m.getName().equals("setMaxConcurrentCallsPerConnection"));
        assertThat(present).as("the shared receiver base must not carry the gRPC-only knob").isFalse();
    }
}
