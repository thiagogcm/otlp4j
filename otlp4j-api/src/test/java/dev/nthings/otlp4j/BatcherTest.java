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
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.ops.OverflowPolicy;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.ProfilesSink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ops.Batcher;
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

@DisplayName("Batcher")
class BatcherTest {

    @DisplayName("Flushes a batch once flushThreshold is reached")
    @Test
    void flushesOnSizeThreshold() {
        var captured = new ArrayList<TracesData>();
        TracesSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(3)
                .queueCapacity(16)
                .build()) {
            for (var i = 0; i < 3; i++) {
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
        var captured = new ArrayList<TracesData>();
        TracesSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(100)
                .queueCapacity(100)
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
        TracesSink downstream = traces -> {
            ignore.incrementAndGet();
            return new CompletableFuture<>();
        };
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(100)
                .queueCapacity(2)
                .overflowPolicy(OverflowPolicy.DROP_NEWEST)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture()
                    .join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture()
                    .join();
            var r = batcher.consume(Fixtures.traceData(Fixtures.span("c", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Partial.class);
            assertThat(batcher.droppedCount()).isEqualTo(1L);
        }
    }

    @DisplayName("DROP_NEWEST reports the rejected OTLP item count, not a Java-batch count of one")
    @Test
    void dropNewestReportsItemCount() {
        // A three-span batch dropped whole must report three rejected spans, not one
        // Java batch.
        var r = dropNewestOverflow(
                Batcher.forTraces().downstream(traces -> new CompletableFuture<>()),
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
        var point = new NumberPoint(Attributes.empty(), 0L, 1L, NumberPoint.longValue(1L), 0L, List.of());
        // Gauge point + four empty-point kinds + a no-data metric: walks every
        // Metric.Data arm, totals 1.
        var dropped = MetricsData.of(Fixtures.checkoutResource(), Fixtures.testScope(), List.of(
                Metric.builder().name("g").data(new Metric.Gauge(List.of(point))).build(),
                Metric.builder().name("s")
                        .data(new Metric.Sum(List.of(), Metric.AggregationTemporality.DELTA,
                                true))
                        .build(),
                Metric.builder().name("h")
                        .data(new Metric.Histogram(List.of(),
                                Metric.AggregationTemporality.CUMULATIVE))
                        .build(),
                Metric.builder().name("e")
                        .data(new Metric.ExponentialHistogram(List.of(),
                                Metric.AggregationTemporality.DELTA))
                        .build(),
                Metric.builder().name("su").data(new Metric.Summary(List.of())).build(),
                Metric.builder().name("nodata").build()));
        var r = dropNewestOverflow(
                Batcher.forMetrics().downstream(metrics -> new CompletableFuture<>()),
                Fixtures.metricsData(Fixtures.metric("warmup")),
                dropped);
        assertThat(r).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                p -> assertThat(p.rejectedItems()).isEqualTo(1L));
    }

    @DisplayName("DROP_NEWEST counts log records")
    @Test
    void dropNewestCountsLogRecords() {
        var r = dropNewestOverflow(
                Batcher.forLogs().downstream(logs -> new CompletableFuture<>()),
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
                Batcher.forProfilesUnsafe().downstream(profiles -> new CompletableFuture<>()),
                Fixtures.profilesData(Fixtures.profile("aa")),
                Fixtures.profilesData(Fixtures.profile("01"), Fixtures.profile("02")));
        assertThat(r).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                p -> assertThat(p.rejectedItems()).isEqualTo(2L));
    }

    @DisplayName("DROP_NEWEST of an item-less batch reports Accepted, not Partial(0)")
    @Test
    void dropNewestOfEmptyBatchReportsAccepted() {
        // A dropped batch carrying no spans loses nothing, so it must not be reported
        // as a partial.
        var r = dropNewestOverflow(
                Batcher.forTraces().downstream(traces -> new CompletableFuture<>()),
                Fixtures.traceData(Fixtures.span("warm", Span.Kind.SERVER)),
                new TracesData(List.of()));
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    /// Fills a capacity-1 DROP_NEWEST batcher (downstream never completes, so
    /// nothing drains), then offers `dropped` into the full queue and returns the
    /// overflow result.
    private static <T> ConsumeResult dropNewestOverflow(
            Batcher.Builder<T> builder, T warmup, T dropped) {
        try (var batcher = builder
                .flushThreshold(100)
                .queueCapacity(1)
                .overflowPolicy(OverflowPolicy.DROP_NEWEST)
                .build()) {
            batcher.consume(warmup).toCompletableFuture().join();
            return batcher.consume(dropped).toCompletableFuture().join();
        }
    }

    @DisplayName("Shutdown drains buffered traces downstream")
    @Test
    void shutdownDrains() {
        var captured = new ArrayList<TracesData>();
        TracesSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(100)
                .queueCapacity(100)
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
        batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(captured).hasSize(1);
    }

    @DisplayName("A downstream-less build()/buildProcessor() succeeds, throws if used as a bare Sink, and batches via Stage.via")
    @Test
    void unboundBuildSucceedsBareSinkThrowsAndViaBatches() {
        // build()/buildProcessor() no longer require a downstream: the result is the mid-chain form.
        try (var built = Batcher.forTraces().flushThreshold(1).build();
                var processor = Batcher.forTraces().flushThreshold(1).buildProcessor()) {
            // Used as a bare Sink with no bound downstream, consume fails fast.
            assertThatThrownBy(() ->
                    built.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("without a bound downstream");
            assertThatThrownBy(() ->
                    processor.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))))
                    .isInstanceOf(IllegalStateException.class);
        }

        // Attached mid-chain via Stage.via, the unbound Batcher batches into the chain continuation.
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TracesSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var batcher = Batcher.forTraces().flushThreshold(2).queueCapacity(16).buildProcessor();
        var sub = Pipeline.from(source).via(batcher).to(terminal);
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            source.dispatch(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> !captured.isEmpty());
        } finally {
            sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        }
        assertThat(captured).hasSize(1);
        assertThat(captured.getFirst().spans()).hasSize(2);
    }

    @DisplayName("shutdown does not report success while an in-flight flush has not completed")
    @Test
    @Timeout(15)
    void shutdownAwaitsInFlightFlushAndTimesOut() {
        // A downstream that never completes: the size-triggered flush stays in flight
        // forever.
        TracesSink stalling = traces -> new CompletableFuture<>();
        var batcher = Batcher.forTraces()
                .downstream(stalling)
                .flushThreshold(1) // flush immediately on the first consume
                .queueCapacity(16)
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
        TracesSink rejecting = traces -> CompletableFuture
                .completedFuture(ConsumeResult.retryable("backend unavailable"));
        var batcher = Batcher.forTraces()
                .downstream(rejecting)
                .flushThreshold(100) // no size-trigger; the queued batch drains on shutdown
                .queueCapacity(100)
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();

        var shutdown = batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture();

        assertThatThrownBy(() -> shutdown.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(Batcher.BatchDeliveryException.class)
                .cause()
                .hasMessageContaining("backend unavailable");
    }

    @DisplayName("shutdown propagates a downstream failure instead of swallowing it")
    @Test
    @Timeout(15)
    void shutdownPropagatesDownstreamFailure() {
        TracesSink failing = traces -> CompletableFuture.failedFuture(new RuntimeException("kaboom"));
        var batcher = Batcher.forTraces()
                .downstream(failing)
                .flushThreshold(100)
                .queueCapacity(100)
                .build();
        batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();

        var shutdown = batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture();

        assertThatThrownBy(() -> shutdown.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .hasMessageContaining("kaboom");
    }

    @DisplayName("forProfilesUnsafe surfaces a distinct-dictionary merge as BatchDeliveryException")
    @Test
    @Timeout(15)
    void shutdownSurfacesDistinctDictionaryMergeFailure() {
        ProfilesSink downstream = profiles -> ConsumeResult.acceptedStage();
        var batcher = Batcher.forProfilesUnsafe()
                .downstream(downstream)
                .flushThreshold(100) // no size-trigger; both batches merge in the shutdown drain
                .queueCapacity(100)
                .build();
        batcher.consume(Fixtures.profilesDataWithDictionary(new byte[] { 1 }, Fixtures.profile("01")))
                .toCompletableFuture().join();
        batcher.consume(Fixtures.profilesDataWithDictionary(new byte[] { 2 }, Fixtures.profile("02")))
                .toCompletableFuture().join();

        var shutdown = batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture();

        assertThatThrownBy(() -> shutdown.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(Batcher.BatchDeliveryException.class)
                .cause()
                .hasMessageContaining("distinct ProfilesDictionaries");
    }

    @DisplayName("Many batchers share one platform timer thread")
    @Test
    void batchersShareOneTimerThread() {
        var batchers = new ArrayList<Batcher<TracesData>>();
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        try {
            for (var i = 0; i < 5; i++) {
                batchers.add(Batcher.forTraces().downstream(downstream).build());
            }
            var timerThreads = Thread.getAllStackTraces().keySet().stream()
                    .filter(t -> t.getName().equals("otlp4j-batch-timer"))
                    .count();
            assertThat(timerThreads).isEqualTo(1L);
        } finally {
            for (var batcher : batchers) {
                batcher.close();
            }
        }
    }
}
