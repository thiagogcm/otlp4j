package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Lifecycle contract for [GrpcOtlpServer] in isolation.
@Timeout(30)
class GrpcOtlpServerLifecycleTest {

    private static final OtlpServerProvider.Dispatchers NO_OP = new OtlpServerProvider.Dispatchers(
            t -> CompletableFuture.completedStage(ConsumeResult.accepted()),
            m -> CompletableFuture.completedStage(ConsumeResult.accepted()),
            l -> CompletableFuture.completedStage(ConsumeResult.accepted()),
            p -> CompletableFuture.completedStage(ConsumeResult.accepted()));

    private static ServerTransportConfig ephemeral() {
        return ServerTransportConfig.builder().port(0).build();
    }

    @Test
    void shutdownBeforeStartIsASafeNoOp() {
        assertThatCode(() -> new GrpcOtlpServer(ephemeral(), NO_OP)
                .shutdown(Duration.ofSeconds(1)).toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @Test
    void shutdownNowBeforeStartIsASafeNoOp() {
        assertThatCode(() -> new GrpcOtlpServer(ephemeral(), NO_OP)
                .shutdownNow().toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @Test
    void portBeforeStartIsZero() {
        assertThat(new GrpcOtlpServer(ephemeral(), NO_OP).port()).isZero();
    }

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
}
