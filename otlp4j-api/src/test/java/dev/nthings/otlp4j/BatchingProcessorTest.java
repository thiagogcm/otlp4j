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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BatchingProcessorTest {

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

    @Test
    void rejectsBuilderWithoutDownstream() {
        var builder = BatchingProcessor.forTraces().maxBatchSize(1);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
    }
}
