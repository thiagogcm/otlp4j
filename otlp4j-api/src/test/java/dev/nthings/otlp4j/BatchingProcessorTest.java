package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.processor.BatchingProcessor;
import dev.nthings.otlp4j.processor.DropPolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    @DisplayName("DROP_NEWEST reports the rejected OTLP item count, not a Java-batch count of one")
    @Test
    void dropNewestReportsItemCount() {
        // A three-span batch dropped whole must report three rejected spans, not one Java batch.
        var r = dropNewestOverflow(
                BatchingProcessor.forTraces().downstream(traces -> new CompletableFuture<>()),
                Fixtures.traceData(Fixtures.span("warm", Span.Kind.SERVER)),
                Fixtures.traceData(
                        Fixtures.span("c", Span.Kind.SERVER),
                        Fixtures.span("d", Span.Kind.SERVER),
                        Fixtures.span("e", Span.Kind.SERVER)));
        assertThat(r).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                p -> assertThat(p.rejectedItems()).isEqualTo(3L));
    }

    @DisplayName("DROP_NEWEST counts metric data points across every data kind")
    @Test
    void dropNewestCountsMetricDataPoints() {
        var point = new NumberPoint(Attributes.empty(), 0L, 1L, NumberPoint.longValue(1L), 0L);
        // Gauge point + four empty-point kinds + a no-data metric: walks every Metric.Data arm, totals 1.
        var dropped = MetricsData.of(Fixtures.checkoutResource(), Fixtures.testScope(), List.of(
                Metric.builder().name("g").data(new Metric.Gauge(List.of(point))).build(),
                Metric.builder().name("s")
                        .data(new Metric.Sum(List.of(), Metric.AggregationTemporality.DELTA, true)).build(),
                Metric.builder().name("h")
                        .data(new Metric.Histogram(List.of(), Metric.AggregationTemporality.CUMULATIVE)).build(),
                Metric.builder().name("e")
                        .data(new Metric.ExponentialHistogram(List.of(), Metric.AggregationTemporality.DELTA)).build(),
                Metric.builder().name("su").data(new Metric.Summary(List.of())).build(),
                new Metric("nodata", "", "", null, Attributes.empty())));
        var r = dropNewestOverflow(
                BatchingProcessor.forMetrics().downstream(metrics -> new CompletableFuture<>()),
                Fixtures.metricsData(Fixtures.metric("warmup")),
                dropped);
        assertThat(r).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                p -> assertThat(p.rejectedItems()).isEqualTo(1L));
    }

    @DisplayName("DROP_NEWEST counts log records")
    @Test
    void dropNewestCountsLogRecords() {
        var r = dropNewestOverflow(
                BatchingProcessor.forLogs().downstream(logs -> new CompletableFuture<>()),
                Fixtures.logsData(Fixtures.logRecord("warm", LogRecord.Severity.INFO)),
                Fixtures.logsData(
                        Fixtures.logRecord("a", LogRecord.Severity.INFO),
                        Fixtures.logRecord("b", LogRecord.Severity.WARN)));
        assertThat(r).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                p -> assertThat(p.rejectedItems()).isEqualTo(2L));
    }

    @DisplayName("DROP_NEWEST counts profiles")
    @Test
    void dropNewestCountsProfiles() {
        var r = dropNewestOverflow(
                BatchingProcessor.forProfiles().downstream(profiles -> new CompletableFuture<>()),
                Fixtures.profilesData(Fixtures.profile("aa")),
                Fixtures.profilesData(Fixtures.profile("01"), Fixtures.profile("02")));
        assertThat(r).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                p -> assertThat(p.rejectedItems()).isEqualTo(2L));
    }

    @DisplayName("DROP_NEWEST of an item-less batch reports Accepted, not Partial(0)")
    @Test
    void dropNewestOfEmptyBatchReportsAccepted() {
        // A dropped batch carrying no spans loses nothing, so it must not be reported as a partial.
        var r = dropNewestOverflow(
                BatchingProcessor.forTraces().downstream(traces -> new CompletableFuture<>()),
                Fixtures.traceData(Fixtures.span("warm", Span.Kind.SERVER)),
                new TraceData(List.of()));
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    /// Fills a capacity-1 DROP_NEWEST batcher (downstream never completes, so nothing drains), then
    /// offers `dropped` into the full queue and returns the overflow result.
    private static <T> ConsumeResult<T> dropNewestOverflow(
            BatchingProcessor.Builder<T> builder, T warmup, T dropped) {
        try (var batcher = builder
                .maxBatchSize(100)
                .queueCapacity(1)
                .maxBatchAge(Duration.ofSeconds(30))
                .dropPolicy(DropPolicy.DROP_NEWEST)
                .build()) {
            batcher.consume(warmup).toCompletableFuture().join();
            return batcher.consume(dropped).toCompletableFuture().join();
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
