package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.processor.OverflowPolicy;
import dev.nthings.otlp4j.pipeline.TraceSink;
import dev.nthings.otlp4j.processor.BatchingProcessor;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("BatchingProcessor overflow policies")
class BatchingPolicyTest {

    @DisplayName("DROP_OLDEST evicts oldest entries when the queue is full")
    @Test
    void dropOldestEvictsAndKeepsLatest() {
        TraceSink stuck = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(stuck)
                .flushThreshold(100)
                .queueCapacity(2)
                .overflowPolicy(OverflowPolicy.DROP_OLDEST)
                .build()) {
            for (var i = 0; i < 5; i++) {
                batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture().join();
            }
            assertThat(batcher.droppedCount()).isGreaterThanOrEqualTo(3L);
        }
    }

    @DisplayName("FAIL policy reports Rejected when the queue is full")
    @Test
    void failPolicyReportsRejected() {
        TraceSink stuck = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(stuck)
                .flushThreshold(100)
                .queueCapacity(2)
                .overflowPolicy(OverflowPolicy.FAIL)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
            var r = batcher.consume(Fixtures.traceData(Fixtures.span("c", Span.Kind.SERVER))).toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
        }
    }

    @DisplayName("forceFlush emits the buffered batch immediately")
    @Test
    void forceFlushDelegatesToFlushNow() {
        var captured = new AtomicReference<TracesData>();
        TraceSink downstream = traces -> {
            captured.set(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .flushThreshold(100)
                .queueCapacity(100)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(captured.get()).isNull();
            batcher.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
            assertThat(captured.get()).isNotNull();
        }
    }

    @DisplayName("flushThreshold of zero throws IllegalArgumentException")
    @Test
    void rejectsZeroFlushThreshold() {
        assertThatThrownBy(() -> BatchingProcessor.forTraces().flushThreshold(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("queueCapacity of zero throws IllegalArgumentException")
    @Test
    void rejectsZeroQueueCapacity() {
        assertThatThrownBy(() -> BatchingProcessor.forTraces().queueCapacity(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("consume after shutdown returns Rejected")
    @Test
    void consumeAfterCloseReturnsRejected() {
        TraceSink downstream = traces -> ConsumeResult.acceptedStage();
        var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .flushThreshold(10)
                .queueCapacity(10)
                .build();
        batcher.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        var r = batcher.consume(Fixtures.traceData(Fixtures.span("late", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
    }
}
