package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ops.AckMode;
import dev.nthings.otlp4j.pipeline.ops.Batcher;
import dev.nthings.otlp4j.pipeline.ops.OverflowPolicy;
import dev.nthings.otlp4j.telemetry.LoggingGatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins F11's Batcher instrument catalog: the `otlp4j.batcher.dropped`/`.drain_failed` counters and
/// the `otlp4j.batcher.queued` gauge are fed alongside the existing getters and the gauge is
/// unregistered on shutdown.
@DisplayName("Batcher self-observability")
class BatcherMetricsTest {

    @DisplayName("the dropped counter tracks queue-overflow drops alongside droppedCount()")
    @Test
    void droppedCounterTracksOverflow() {
        var registry = new LoggingGatewayMetrics();
        TracesSink stuck = traces -> new CompletableFuture<>();
        try (var batcher = Batcher.forTraces()
                .downstream(stuck)
                .telemetry(registry)
                .ackMode(AckMode.ON_ENQUEUE)
                .flushThreshold(100)
                .queueCapacity(2)
                .overflowPolicy(OverflowPolicy.FAIL)
                .build()) {
            for (var i = 0; i < 5; i++) {
                batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture().join();
            }
            assertThat(batcher.droppedCount()).isPositive();
            assertThat(registry.counterValue("otlp4j.batcher.dropped")).isEqualTo(batcher.droppedCount());
        }
    }

    @DisplayName("the drain_failed counter tracks a failed downstream drain alongside drainFailedCount()")
    @Test
    void drainFailedCounterTracksFailedDrain() {
        var registry = new LoggingGatewayMetrics();
        TracesSink failing = traces ->
                CompletableFuture.failedFuture(new IllegalStateException("downstream down"));
        try (var batcher = Batcher.forTraces()
                .downstream(failing)
                .telemetry(registry)
                .ackMode(AckMode.ON_ENQUEUE)
                .flushThreshold(1)
                .queueCapacity(10)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            batcher.forceFlush(Duration.ofSeconds(1)).toCompletableFuture()
                    .handle((v, t) -> null).join();
            assertThat(batcher.drainFailedCount()).isPositive();
            assertThat(registry.counterValue("otlp4j.batcher.drain_failed"))
                    .isEqualTo(batcher.drainFailedCount());
        }
    }

    @DisplayName("the queued gauge reflects queue depth and is unregistered on shutdown")
    @Test
    void queuedGaugeReflectsDepthThenUnregisters() {
        var registry = new LoggingGatewayMetrics();
        TracesSink stuck = traces -> new CompletableFuture<>();
        var batcher = Batcher.forTraces()
                .downstream(stuck)
                .telemetry(registry)
                .ackMode(AckMode.ON_ENQUEUE)
                .flushThreshold(100)
                .queueCapacity(100)
                .build();
        for (var i = 0; i < 3; i++) {
            batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                    .toCompletableFuture().join();
        }
        assertThat(batcher.queued()).isEqualTo(3);
        assertThat(registry.gaugeValue("otlp4j.batcher.queued")).isEqualTo(batcher.queued());

        batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture()
                .handle((v, t) -> null).join();
        // The gauge registration is closed on shutdown, so the dead batcher is no longer polled.
        assertThat(registry.gaugeValue("otlp4j.batcher.queued")).isZero();
    }

    @DisplayName("the default (noop) telemetry leaves the getters working and adds no metrics")
    @Test
    void defaultTelemetryIsInert() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(1)
                .queueCapacity(10)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(batcher.droppedCount()).isZero();
        }
    }
}
