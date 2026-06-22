package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.processor.BatchingProcessor;
import dev.nthings.otlp4j.processor.DropPolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("BatchingProcessor")
class BatchingProcessorTest {

    @DisplayName("Flushes a batch once maxBatchSize is reached")
    @Test
    void flushesOnSizeThreshold() {
        var captured = new ArrayList<TraceData>();
        TraceConsumer downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(3)
                .queueCapacity(16)
                .maxBatchAge(Duration.ofSeconds(30))
                .build()) {
            for (int i = 0; i < 3; i++) {
                batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture().join();
            }
            await().atMost(Duration.ofSeconds(2)).until(() -> !captured.isEmpty());
        }
        assertThat(captured).hasSize(1);
        assertThat(captured.getFirst().spans()).hasSize(3);
    }

    @DisplayName("Flushes a batch once maxBatchAge elapses")
    @Test
    void flushesOnAgeThreshold() {
        var captured = new ArrayList<TraceData>();
        TraceConsumer downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(100)
                .queueCapacity(100)
                .maxBatchAge(Duration.ofMillis(100))
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> !captured.isEmpty());
        }
        assertThat(captured).hasSize(1);
    }

    @DisplayName("DROP_NEWEST reports Partial and increments droppedCount")
    @Test
    void dropNewestReportsPartialSuccess() {
        var ignore = new AtomicInteger();
        TraceConsumer downstream = traces -> {
            ignore.incrementAndGet();
            return new CompletableFuture<>();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(100)
                .queueCapacity(2)
                .maxBatchAge(Duration.ofSeconds(30))
                .dropPolicy(DropPolicy.DROP_NEWEST)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
            var r = batcher.consume(Fixtures.traceData(Fixtures.span("c", Span.Kind.SERVER))).toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Partial.class);
            assertThat(batcher.droppedCount()).isEqualTo(1L);
        }
    }

    @DisplayName("Shutdown drains buffered traces downstream")
    @Test
    void shutdownDrains() {
        var captured = new ArrayList<TraceData>();
        TraceConsumer downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(100)
                .queueCapacity(100)
                .maxBatchAge(Duration.ofSeconds(30))
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
        batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(captured).hasSize(1);
    }

    @DisplayName("Builder without downstream throws IllegalStateException")
    @Test
    void rejectsBuilderWithoutDownstream() {
        var builder = BatchingProcessor.forTraces().maxBatchSize(1);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
    }

    @DisplayName("shutdown does not report success while an in-flight flush has not completed")
    @Test
    @Timeout(15)
    void shutdownAwaitsInFlightFlushAndTimesOut() {
        // A downstream that never completes: the size-triggered flush stays in flight forever.
        TraceConsumer stalling = traces -> new CompletableFuture<>();
        var batcher = BatchingProcessor.forTraces()
                .downstream(stalling)
                .maxBatchSize(1) // flush immediately on the first consume
                .queueCapacity(16)
                .maxBatchAge(Duration.ofSeconds(30))
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();

        var shutdown = batcher.shutdown(Duration.ofMillis(300)).toCompletableFuture();

        // Old code reported instant success; the redesign surfaces the timeout.
        assertThatThrownBy(() -> shutdown.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @DisplayName("shutdown propagates a downstream rejection instead of swallowing it")
    @Test
    @Timeout(15)
    void shutdownPropagatesDownstreamRejection() {
        TraceConsumer rejecting = traces ->
                CompletableFuture.completedFuture(ConsumeResult.rejected("backend unavailable"));
        var batcher = BatchingProcessor.forTraces()
                .downstream(rejecting)
                .maxBatchSize(100) // no size-trigger; the queued batch drains on shutdown
                .queueCapacity(100)
                .maxBatchAge(Duration.ofSeconds(30))
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();

        var shutdown = batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture();

        assertThatThrownBy(() -> shutdown.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(BatchingProcessor.BatchDeliveryException.class)
                .cause()
                .hasMessageContaining("backend unavailable");
    }

    @DisplayName("shutdown propagates a downstream failure instead of swallowing it")
    @Test
    @Timeout(15)
    void shutdownPropagatesDownstreamFailure() {
        TraceConsumer failing = traces -> CompletableFuture.failedFuture(new RuntimeException("kaboom"));
        var batcher = BatchingProcessor.forTraces()
                .downstream(failing)
                .maxBatchSize(100)
                .queueCapacity(100)
                .maxBatchAge(Duration.ofSeconds(30))
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();

        var shutdown = batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture();

        assertThatThrownBy(() -> shutdown.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .hasMessageContaining("kaboom");
    }
}
