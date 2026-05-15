package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Lifecycle contract for [GrpcOtlpServer] in isolation — exercises the guard clauses and
/// idempotent shutdown paths without standing up a real channel. The end-to-end SPI wiring is
/// covered by `GrpcTransportTest`.
@Timeout(30)
class GrpcOtlpServerLifecycleTest {

    private static final TelemetryConsumer NO_OP = new TelemetryConsumer() {
        @Override
        public ExportResult consumeTraces(TraceData traces) {
            return ExportResult.success();
        }
    };

    @Test
    void shutdownBeforeStartIsASafeNoOp() {
        assertThatCode(() -> new GrpcOtlpServer(NO_OP).shutdown())
                .as("shutdown() before start() must be a no-op, not an error")
                .doesNotThrowAnyException();
    }

    @Test
    void shutdownNowBeforeStartIsASafeNoOp() {
        assertThatCode(() -> new GrpcOtlpServer(NO_OP).shutdownNow())
                .as("shutdownNow() before start() must be a no-op, not an error")
                .doesNotThrowAnyException();
    }

    @Test
    void portBeforeStartThrowsIllegalState() {
        assertThatThrownBy(() -> new GrpcOtlpServer(NO_OP).port())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server not started");
    }

    @Test
    void awaitTerminationBeforeStartThrowsIllegalState() {
        assertThatThrownBy(() -> new GrpcOtlpServer(NO_OP).awaitTermination(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server not started");
    }

    @Test
    void awaitTerminationNoArgBeforeStartThrowsIllegalState() {
        assertThatThrownBy(() -> new GrpcOtlpServer(NO_OP).awaitTermination())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server not started");
    }

    @Test
    void doubleStartThrowsIllegalState() throws Exception {
        var server = new GrpcOtlpServer(NO_OP);
        server.start(0);
        try {
            assertThat(server.port()).isPositive();
            assertThatThrownBy(() -> server.start(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("server already started");
        } finally {
            server.shutdownNow();
            server.awaitTermination(Duration.ofSeconds(5));
        }
    }
}
