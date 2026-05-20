package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.processor.BatchingProcessor;
import dev.nthings.otlp4j.processor.DropPolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

class BatchingPolicyTest {

    @Test
    void dropOldestEvictsAndKeepsLatest() {
        TraceConsumer stuck = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(stuck)
                .maxBatchSize(100)
                .queueCapacity(2)
                .maxBatchAge(Duration.ofSeconds(30))
                .dropPolicy(DropPolicy.DROP_OLDEST)
                .build()) {
            for (int i = 0; i < 5; i++) {
                batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture().join();
            }
            assertThat(batcher.droppedCount()).isGreaterThanOrEqualTo(3L);
        }
    }

    @Test
    void errorPolicyReportsRejected() {
        TraceConsumer stuck = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(stuck)
                .maxBatchSize(100)
                .queueCapacity(2)
                .maxBatchAge(Duration.ofSeconds(30))
                .dropPolicy(DropPolicy.ERROR)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
            var r = batcher.consume(Fixtures.traceData(Fixtures.span("c", Span.Kind.SERVER))).toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
        }
    }

    @Test
    void externalDropCounterReceivesEvents() {
        var counter = new LongAdder();
        TraceConsumer stuck = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(stuck)
                .maxBatchSize(100)
                .queueCapacity(1)
                .maxBatchAge(Duration.ofSeconds(30))
                .dropPolicy(DropPolicy.DROP_NEWEST)
                .dropCounter(counter)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("c", Span.Kind.SERVER))).toCompletableFuture().join();
            assertThat(counter.sum()).isGreaterThan(0L);
        }
    }

    @Test
    void forceFlushDelegatesToFlushNow() {
        var captured = new AtomicReference<TraceData>();
        TraceConsumer downstream = traces -> {
            captured.set(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(100)
                .queueCapacity(100)
                .maxBatchAge(Duration.ofSeconds(30))
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(captured.get()).isNull();
            batcher.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
            assertThat(captured.get()).isNotNull();
        }
    }

    @Test
    void rejectsZeroMaxBatchSize() {
        assertThatThrownBy(() -> BatchingProcessor.forTraces().maxBatchSize(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsZeroQueueCapacity() {
        assertThatThrownBy(() -> BatchingProcessor.forTraces().queueCapacity(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsZeroMaxBatchAge() {
        assertThatThrownBy(() -> BatchingProcessor.forTraces().maxBatchAge(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void consumeAfterCloseReturnsRejected() {
        TraceConsumer downstream = traces -> ConsumeResult.acceptedStage();
        var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(10)
                .queueCapacity(10)
                .maxBatchAge(Duration.ofSeconds(30))
                .build();
        batcher.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        var r = batcher.consume(Fixtures.traceData(Fixtures.span("late", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
    }
}
