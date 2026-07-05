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
import dev.nthings.otlp4j.pipeline.ops.AckMode;
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
import java.util.concurrent.atomic.AtomicReference;
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
                .build()) {   // ON_DELIVERY default
            // Fire all three before joining: under ON_DELIVERY a per-batch join below the threshold
            // would deadlock, since batch i's ack cannot resolve until the queue reaches the
            // threshold. Firing all three lets one coalesced delivery resolve every ack Accepted.
            var acks = new ArrayList<CompletableFuture<ConsumeResult>>();
            for (var i = 0; i < 3; i++) {
                acks.add(batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture());
            }
            for (var ack : acks) {
                assertThat(ack.join()).isInstanceOf(ConsumeResult.Accepted.class);
            }
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
                // The downstream never completes; ON_ENQUEUE keeps the warmup joins from blocking on a
                // drain, so the test asserts DROP_NEWEST overflow (enqueue-time) mechanics.
                .ackMode(AckMode.ON_ENQUEUE)
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
                // The warmup downstream never completes; ON_ENQUEUE keeps the warmup join from
                // blocking on a drain, so the test asserts overflow (enqueue-time) mechanics.
                .ackMode(AckMode.ON_ENQUEUE)
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
            // Fire both before joining: under the ON_DELIVERY default a per-dispatch join below the
            // flushThreshold would resolve only via the age timer, draining each singly. Firing both
            // lets the queue reach the threshold and coalesce into one delivery.
            var a = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture();
            var b = source.dispatch(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)))
                    .toCompletableFuture();
            a.join();
            b.join();
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
                // ON_ENQUEUE: the point is the shutdown timeout, not the ack; a never-completing
                // downstream would otherwise block the first consume join under ON_DELIVERY.
                .ackMode(AckMode.ON_ENQUEUE)
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
                // ON_ENQUEUE: this test asserts that the SHUTDOWN drain propagates a rejection. Under
                // ON_DELIVERY the consume join would force the age timer to drain the single
                // sub-threshold batch first, emptying the queue before shutdown runs. ON_ENQUEUE keeps
                // the batch queued for the shutdown drain (the Void exception shape is identical).
                .ackMode(AckMode.ON_ENQUEUE)
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
                // ON_ENQUEUE: asserts the SHUTDOWN drain propagates a downstream failure with the raw
                // cause preserved; keeps the batch queued for the shutdown drain (see rejection test).
                .ackMode(AckMode.ON_ENQUEUE)
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
                // ON_ENQUEUE: both batches must sit in the queue together so the SHUTDOWN drain merges
                // them and surfaces the distinct-dictionary conflict. Under ON_DELIVERY each consume
                // join would drain its batch singly on the age timer (a single batch never conflicts).
                .ackMode(AckMode.ON_ENQUEUE)
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

    @DisplayName("ON_DELIVERY: the ack reflects the downstream result verbatim")
    @Test
    @Timeout(15)
    void onDeliveryAckReflectsDownstreamResult() {
        var next = new AtomicReference<ConsumeResult>(ConsumeResult.accepted());
        TracesSink downstream = traces -> CompletableFuture.completedFuture(next.get());
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(1) // each batch drains on its own; the ack carries the real verdict
                .queueCapacity(16)
                .build()) {   // ON_DELIVERY default
            assertThat(consumeJoin(batcher)).isInstanceOf(ConsumeResult.Accepted.class);

            next.set(ConsumeResult.partial(2, "half"));
            assertThat(consumeJoin(batcher)).isInstanceOfSatisfying(ConsumeResult.Partial.class,
                    p -> assertThat(p.rejectedItems()).isEqualTo(2L));

            next.set(ConsumeResult.retryable("try later"));
            assertThat(consumeJoin(batcher)).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    r -> assertThat(r.message()).contains("try later"));
        }
    }

    @DisplayName("ON_DELIVERY: merged batches share one delivery fate")
    @Test
    @Timeout(15)
    void onDeliveryMergedBatchesShareFate() {
        TracesSink downstream = traces -> CompletableFuture
                .completedFuture(ConsumeResult.retryable("whole batch rejected"));
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .flushThreshold(3) // three inputs coalesce into one downstream consume
                .queueCapacity(16)
                .build()) {   // ON_DELIVERY default
            var acks = new ArrayList<CompletableFuture<ConsumeResult>>();
            for (var i = 0; i < 3; i++) {
                acks.add(batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture());
            }
            for (var ack : acks) {
                assertThat(ack.join()).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                        r -> assertThat(r.message()).contains("whole batch rejected"));
            }
        }
    }

    @DisplayName("ON_DELIVERY: a downstream that throws acks the caller retryably")
    @Test
    @Timeout(15)
    void onDeliveryDownstreamThrowAcksRetryable() {
        TracesSink throwing = traces -> {
            throw new RuntimeException("kaboom");
        };
        try (var batcher = Batcher.forTraces()
                .downstream(throwing)
                .flushThreshold(1)
                .queueCapacity(16)
                .build()) {   // ON_DELIVERY default
            var r = batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(r).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rr -> {
                assertThat(rr.retryable()).isTrue();
                assertThat(rr.cause()).hasMessageContaining("kaboom");
            });
        }
    }

    @DisplayName("drainFailedCount counts failed-drain batches; droppedCount stays 0")
    @Test
    @Timeout(15)
    void drainFailedCountCountsFailedDrains() {
        TracesSink rejecting = traces -> CompletableFuture
                .completedFuture(ConsumeResult.retryable("backend down"));
        try (var batcher = Batcher.forTraces()
                .downstream(rejecting)
                .ackMode(AckMode.ON_ENQUEUE) // fire-and-forget: the failed drain is silent loss
                .flushThreshold(1)
                .queueCapacity(16)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(3)).until(() -> batcher.drainFailedCount() >= 1L);
            assertThat(batcher.drainFailedCount()).isEqualTo(1L);
            assertThat(batcher.droppedCount()).isZero();
        }
    }

    @DisplayName("ON_DELIVERY: DROP_OLDEST completes the evicted caller's ack retryably")
    @Test
    @Timeout(15)
    void dropOldestCompletesEvictedAckRetryable() {
        TracesSink stuck = traces -> new CompletableFuture<>();
        try (var batcher = Batcher.forTraces()
                .downstream(stuck)
                .flushThreshold(100)
                .queueCapacity(1)
                .overflowPolicy(OverflowPolicy.DROP_OLDEST)
                .build()) {   // ON_DELIVERY default
            var first = batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture();               // enqueued, ack pending
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)))
                    .toCompletableFuture();               // evicts the first entry

            assertThat(first.join()).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
                assertThat(r.retryable()).isTrue();
                assertThat(r.message()).contains("DROP_OLDEST");
            });
            assertThat(batcher.droppedCount()).isEqualTo(1L);
        }
    }

    @DisplayName("ON_ENQUEUE: DROP_OLDEST eviction leaves the null ack untouched")
    @Test
    @Timeout(15)
    void dropOldestUnderOnEnqueueDoesNotTouchAck() {
        TracesSink stuck = traces -> new CompletableFuture<>();
        try (var batcher = Batcher.forTraces()
                .downstream(stuck)
                .ackMode(AckMode.ON_ENQUEUE)
                .flushThreshold(100)
                .queueCapacity(1)
                .overflowPolicy(OverflowPolicy.DROP_OLDEST)
                .build()) {
            var first = batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(first).isInstanceOf(ConsumeResult.Accepted.class);
            var second = batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)))
                    .toCompletableFuture().join();   // evicts the first entry, whose ack is null
            assertThat(second).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(batcher.droppedCount()).isEqualTo(1L);
        }
    }

    @DisplayName("flushInterval knob drives the age-based flush faster than the default")
    @Test
    @Timeout(15)
    void flushIntervalKnobDrivesTimer() {
        var captured = new AtomicInteger();
        TracesSink downstream = traces -> {
            captured.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = Batcher.forTraces()
                .downstream(downstream)
                .ackMode(AckMode.ON_ENQUEUE)
                .flushInterval(Duration.ofMillis(100)) // well below the 1s default
                .flushThreshold(1000)
                .queueCapacity(1000)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            // The age timer drains within ~100ms; the 1s default could not meet this deadline.
            await().atMost(Duration.ofMillis(800)).until(() -> captured.get() == 1);
        }
    }

    @DisplayName("ON_DELIVERY: deliveryTimeout fails a hung downstream instead of stranding the ack")
    @Test
    @Timeout(15)
    void deliveryTimeoutFailsHungDownstream() {
        TracesSink hung = traces -> new CompletableFuture<>();
        try (var batcher = Batcher.forTraces()
                .downstream(hung)
                .deliveryTimeout(Duration.ofMillis(200))
                .flushThreshold(1)
                .queueCapacity(16)
                .build()) {   // ON_DELIVERY default
            var r = batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(r).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rr -> {
                assertThat(rr.retryable()).isTrue();
                assertThat(rr.cause()).isInstanceOf(TimeoutException.class);
            });
        }
    }

    private static ConsumeResult consumeJoin(Batcher<TracesData> batcher) {
        return batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
    }
}
