package dev.nthings.otlp4j.core.delivery;

import static dev.nthings.otlp4j.core.delivery.DeliverySupport.CTX;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.FIVE_SECONDS;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.PROV;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DeliveryQueueFailureTest {


    @Test
    void concurrentWorkersKeepLaterAdmissionsOutsideAnEarlierFlush() throws Exception {
        var first = new CompletableFuture<DeliveryResult>();
        var second = new CompletableFuture<DeliveryResult>();
        var later = new CompletableFuture<DeliveryResult>();
        var firstTwoEntered = new CountDownLatch(2);
        var laterEntered = new CountDownLatch(1);
        var calls = new AtomicInteger();
        SignalConsumer<TracesData> next = (context, payload) -> {
            return switch (calls.getAndIncrement()) {
                case 0 -> { firstTwoEntered.countDown(); yield first; }
                case 1 -> { firstTwoEntered.countDown(); yield second; }
                case 2 -> { laterEntered.countDown(); yield later; }
                default -> throw new AssertionError("unexpected delivery");
            };
        };
        var q = queue(new DeliveryQueuePolicy(4, 2, OverflowPolicy.FAIL), next);
        q.start();
        try {
            q.offer(CTX, traces(2));
            q.offer(CTX, traces(2));
            assertThat(firstTwoEntered.await(2, TimeUnit.SECONDS)).isTrue();
            var flush = q.flush(FIVE_SECONDS).toCompletableFuture();
            q.offer(CTX, traces(3));
            second.complete(DeliveryResult.partial(1, "earlier rejection", PROV));
            assertThat(laterEntered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(flush).isNotDone();

            first.complete(DeliveryResult.accepted(PROV));
            assertThat(flush.get(2, TimeUnit.SECONDS).rejectedItems()).isEqualTo(1);
            assertThat(later).isNotDone();
            later.complete(DeliveryResult.partial(2, "later rejection", PROV));
            assertThat(q.flush(FIVE_SECONDS).toCompletableFuture().get(2, TimeUnit.SECONDS)
                    .rejectedItems()).isEqualTo(2);
        } finally {
            first.complete(DeliveryResult.accepted(PROV));
            second.complete(DeliveryResult.accepted(PROV));
            later.complete(DeliveryResult.accepted(PROV));
            q.drainAndStop(FIVE_SECONDS).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void flushWaitsForAdmissionReleaseAfterDownstreamCompletion(boolean fatal) throws Exception {
        var releaseEntered = new CompletableFuture<Void>();
        var releaseAllowed = new CompletableFuture<Void>();
        var released = new AtomicInteger();
        SignalConsumer<TracesData> next = (context, payload) -> {
            if (fatal) {
                throw new AssertionError("worker failed");
            }
            return DeliveryResult.acceptedStage(PROV);
        };
        var q = queue(policy(2, OverflowPolicy.FAIL), next);
        q.start();
        try {
            var admission = q.offerAll(CTX, List.of(traces(1)), () -> {
                releaseEntered.complete(null);
                releaseAllowed.join();
                released.incrementAndGet();
            });
            assertThat(admission.result()).isInstanceOf(DeliveryResult.Accepted.class);
            releaseEntered.get(2, TimeUnit.SECONDS);
            var flush = q.flush(FIVE_SECONDS).toCompletableFuture();
            // Keep release blocked while establishing that the barrier also waits for ownership.
            assertThatThrownBy(() -> flush.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            releaseAllowed.complete(null);
            var report = flush.get(2, TimeUnit.SECONDS);
            assertThat(released).hasValue(1);
            assertThat(report.rejectedItems()).isEqualTo(fatal ? 1 : 0);
        } finally {
            releaseAllowed.complete(null);
            q.drainAndStop(FIVE_SECONDS).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static DeliveryQueuePolicy policy(int queueSize, OverflowPolicy overflow) {
        return new DeliveryQueuePolicy(queueSize, 1, overflow);
    }

    private static <T> DeliveryQueue<T> queue(DeliveryQueuePolicy policy, SignalConsumer<T> next) {
        return new DeliveryQueue<>(policy, next, PROV, (items, reason) -> {});
    }

    private static void assertFlushTimeout(CompletionStage<FlushReport> stage) {
        assertThatThrownBy(() -> stage.toCompletableFuture().join())
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void failOverflowRejectsAnEntireGroupThatExceedsCapacity() {
        var next = new DeliverySupport.Recording();
        var completions = new AtomicInteger();
        var q = queue(policy(1, OverflowPolicy.FAIL), next);
        q.start();

        var admission = q.offerAll(
                CTX, List.of(traces(1), traces(2)), completions::incrementAndGet);

        assertThat(admission.result()).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) admission.result();
        assertThat(rejected.reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(rejected.retryAfter()).isNotNull();
        assertThat(completions).hasValue(0);
        assertThat(next.received()).isEmpty();

        // A refusal is returned to the caller, which owns the retry; the queue records nothing for it.
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropNewestAcknowledgesAndDropsAnEntireGroupThatExceedsCapacity() {
        var next = new DeliverySupport.Recording();
        var completions = new AtomicInteger();
        var lostItems = new AtomicLong();
        var q = new DeliveryQueue<>(
                policy(1, OverflowPolicy.DROP_NEWEST), next, PROV, (items, reason) -> lostItems.addAndGet(items));
        q.start();

        var admission = q.offerAll(
                CTX, List.of(traces(1), traces(2)), completions::incrementAndGet);

        assertThat(admission.result()).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(admission.stages()).hasSize(2);
        assertThat(completions).hasValue(1);
        assertThat(lostItems).hasValue(3);
        assertThat(next.received()).isEmpty();

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isZero();
        assertThat(report.droppedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void nullDownstreamResultBecomesARecordedWholeBatchFailure() {
        SignalConsumer<TracesData> next = (context, payload) -> CompletableFuture.completedFuture(null);
        var q = queue(policy(2, OverflowPolicy.FAIL), next);
        q.start();

        assertThat(q.offer(CTX, traces(2)).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(report.firstFailure())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("null delivery result");
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void repeatedTimeoutsCoalesceCarryAndPreserveBothLateFailures() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.always(DeliveryResult.permanent("late failure", PROV));
        var q = queue(policy(8, OverflowPolicy.FAIL), next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertFlushTimeout(q.flush(Duration.ofMillis(10)));

        q.offer(CTX, traces(2));
        assertFlushTimeout(q.flush(Duration.ofMillis(10)));

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.droppedItems()).isZero();
        assertThat(report.failedBatches()).isEqualTo(2);
        assertThat(report.firstFailure()).isNotNull();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void fatalDownstreamErrorRunsTheGroupedCompletionAndReleasesItsEntry() {
        var fatal = new AssertionError("fatal downstream failure");
        SignalConsumer<TracesData> next = (context, payload) -> {
            throw fatal;
        };
        var completions = new AtomicInteger();
        var q = queue(policy(2, OverflowPolicy.FAIL), next);
        q.start();

        var admission = q.offerAll(CTX, List.of(traces(1)), completions::incrementAndGet);
        assertThat(admission.result()).isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(completions).hasValue(1);
        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(report.firstFailure()).isSameAs(fatal);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropOldestSeparatesEvictionsFromDownstreamPartialRejections() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.always(DeliveryResult.partial(1, "partial", PROV));
        var q = queue(policy(2, OverflowPolicy.DROP_OLDEST), next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));
        q.offer(CTX, traces(4));

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.droppedItems()).isEqualTo(2);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(8);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropNewestCombinesADroppedGroupWithPartialDownstreamResults() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.always(DeliveryResult.partial(1, "partial", PROV));
        var q = queue(policy(1, OverflowPolicy.DROP_NEWEST), next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var dropped = q.offer(CTX, traces(3)).toCompletableFuture().join();
        assertThat(dropped).isInstanceOf(DeliveryResult.Accepted.class);

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(report.droppedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void shutdownTimeoutLeavesLateOutcomeAvailableThroughTheNextFlush() throws InterruptedException {
        var delivered = new CompletableFuture<DeliveryResult>();
        var received = new CountDownLatch(1);
        SignalConsumer<TracesData> next = (context, payload) -> {
            received.countDown();
            return delivered;
        };
        var q = queue(policy(2, OverflowPolicy.FAIL), next);
        q.start();
        q.offer(CTX, traces(1));
        try {
            assertThat(received.await(2, TimeUnit.SECONDS)).isTrue();
            assertFlushTimeout(q.drainAndStop(Duration.ofMillis(20)));
        } finally {
            delivered.complete(DeliveryResult.accepted(PROV));
        }

        var lateReport = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(lateReport.isSuccess()).isTrue();
    }
}
