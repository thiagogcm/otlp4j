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
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class DeliveryQueueTest {


    private static DeliveryQueuePolicy policy(int queueSize, OverflowPolicy overflow) {
        return new DeliveryQueuePolicy(queueSize, 1, overflow);
    }

    private static DeliveryQueue<TracesData> queue(
            DeliveryQueuePolicy policy, DeliverySupport.Recording next) {
        return queue(policy, next, new AtomicLong());
    }

    /// Builds a queue whose loss callback adds into `lost`, so a test can assert what was reported.
    private static DeliveryQueue<TracesData> queue(
            DeliveryQueuePolicy policy, DeliverySupport.Recording next, AtomicLong lost) {
        return new DeliveryQueue<>(policy, next, PROV, (items, reason) -> lost.addAndGet(items));
    }

    private static DeliveryResult join(CompletionStage<DeliveryResult> stage) {
        return stage.toCompletableFuture().join();
    }

    @Test
    void acknowledgementIsAdmissionAndFlushObservesPartialResult() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.partial(1, "one", PROV));
        var q = queue(policy(8, OverflowPolicy.FAIL), next);
        q.start();

        assertThat(join(q.offer(CTX, traces(3)))).isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isZero();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void groupedAdmissionUsesConfiguredCapacityNotCurrentOccupancy() {
        var next = new DeliverySupport.Recording();
        var q = queue(policy(2, OverflowPolicy.FAIL), next);
        q.start();

        var admission = q.offerAll(CTX, List.of(traces(1), traces(2)), () -> {});
        assertThat(admission.result()).isInstanceOf(DeliveryResult.Accepted.class);

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        assertThat(next.totalItems()).isEqualTo(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void wholeDownstreamFailureCountsAllPayloadItems() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.permanent("downstream down", PROV));
        var q = queue(policy(8, OverflowPolicy.FAIL), next);
        q.start();

        q.offer(CTX, traces(3));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void aRefusedGroupBelongsToItsCallerAndIsNotInTheNextFlushReport() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var lost = new AtomicLong();
        var q = queue(policy(2, OverflowPolicy.FAIL), next, lost);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(1));
        var refused = q.offerAll(CTX, List.of(traces(2), traces(3)), () -> {});
        assertThat(refused.result()).isInstanceOf(DeliveryResult.Rejected.class);

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        // The sender was told and retries; counting the refusal here would report a loss for data it
        // delivers on the retry.
        assertThat(report.isSuccess()).isTrue();
        assertThat(lost).hasValue(0);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void warningOnlyPartialResultIsSuccessful() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.partial(0, "accepted with warning", PROV));
        var q = queue(policy(8, OverflowPolicy.FAIL), next);
        q.start();

        q.offer(CTX, traces(1));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void failOverflowRefusesWithoutEnteringPendingBarrier() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.FAIL), next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var refusal = join(q.offer(CTX, traces(3)));
        assertThat(refusal).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refusal).reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(((DeliveryResult.Rejected) refusal).retryAfter()).isNotNull();

        gate.countDown();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropOldestIsReportedAtFlushAndDoesNotPartiallyAdmitASplitGroup() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(2, OverflowPolicy.DROP_OLDEST), next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));
        q.offerAll(CTX, List.of(traces(4), traces(5)), () -> {});

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.droppedItems()).isEqualTo(5);
        assertThat(next.totalItems()).isEqualTo(10);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropNewestAcknowledgesAndRecordsTheDropWithoutPartialAdmission() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var lost = new AtomicLong();
        var q = queue(policy(1, OverflowPolicy.DROP_NEWEST), next, lost);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var completions = new AtomicInteger();
        var dropped = q.offerAll(CTX, List.of(traces(3)), completions::incrementAndGet);
        // The sender is told it succeeded, so the drop is the queue's loss to count, and its completion
        // callback runs because nothing else will.
        assertThat(dropped.result()).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(completions).hasValue(1);
        assertThat(lost).hasValue(3);

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isZero();
        assertThat(report.droppedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @ParameterizedTest
    @EnumSource(OverflowPolicy.class)
    void eachOverflowModeReturnsItsResultAndAccountsForItsLosses(OverflowPolicy overflow) {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var lost = new AtomicLong();
        var q = queue(policy(1, overflow), next, lost);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(join(q.offer(CTX, traces(2)))).isInstanceOf(DeliveryResult.Accepted.class);
        var overflowing = join(q.offer(CTX, traces(4)));
        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        switch (overflow) {
            case FAIL -> {
                // Refused to the caller: no record, no loss, and the refused payload never ran.
                assertThat(overflowing).isInstanceOfSatisfying(DeliveryResult.Rejected.class, refusal -> {
                    assertThat(refusal.disposition()).isEqualTo(Disposition.RETRYABLE);
                    assertThat(refusal.retryAfter()).isNotNull();
                });
                assertThat(report.isSuccess()).isTrue();
                assertThat(lost).hasValue(0);
                assertThat(next.totalItems()).isEqualTo(3);
            }
            case DROP_NEWEST -> {
                // Acknowledged then discarded: the arriving payload is the loss.
                assertThat(overflowing).isInstanceOf(DeliveryResult.Accepted.class);
                assertThat(report.droppedItems()).isEqualTo(4);
                assertThat(lost).hasValue(4);
                assertThat(next.totalItems()).isEqualTo(3);
            }
            case DROP_OLDEST -> {
                // Acknowledged, and the oldest waiting payload is the loss.
                assertThat(overflowing).isInstanceOf(DeliveryResult.Accepted.class);
                assertThat(report.droppedItems()).isEqualTo(2);
                assertThat(lost).hasValue(2);
                assertThat(next.totalItems()).isEqualTo(5);
            }
        }
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void fatalWorkerRejectsQueuedOwnershipAndClosesAdmission() throws InterruptedException {
        var fatal = new AssertionError("fatal downstream failure");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        SignalConsumer<TracesData> next = (context, payload) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw fatal;
            }
            throw fatal;
        };
        var q = new DeliveryQueue<>(policy(2, OverflowPolicy.FAIL), next, PROV, (items, reason) -> {});
        q.start();

        q.offer(CTX, traces(1));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        q.offer(CTX, traces(2));
        release.countDown();

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(2);
        assertThat(join(q.offer(CTX, traces(1)))).satisfies(result -> {
            assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
            assertThat(((DeliveryResult.Rejected) result).reason()).isEqualTo(DeliveryReason.SHUTDOWN);
        });
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void flushTimeoutPreservesLateOutcomeAndShutdownRefusesNewAdmission() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.always(DeliveryResult.permanent("late downstream failure", PROV));
        var q = queue(policy(8, OverflowPolicy.FAIL), next);
        q.start();
        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);

        assertThatThrownBy(() -> q.flush(Duration.ofMillis(10)).toCompletableFuture().join())
                .hasRootCauseInstanceOf(TimeoutException.class);
        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(join(q.offer(CTX, traces(1)))).satisfies(result -> {
            assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
            assertThat(((DeliveryResult.Rejected) result).reason()).isEqualTo(DeliveryReason.SHUTDOWN);
        });
    }

    @Test
    void aFailedDeliveryDoesNotStopLaterEntriesFromBeingAttempted() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.permanent("downstream down", PROV));
        var lost = new AtomicLong();
        var q = queue(policy(8, OverflowPolicy.FAIL), next, lost);
        q.start();

        for (var i = 0; i < 5; i++) {
            assertThat(join(q.offer(CTX, traces(2)))).isInstanceOf(DeliveryResult.Accepted.class);
            q.flush(FIVE_SECONDS).toCompletableFuture().join();
        }

        assertThat(next.received()).hasSize(5);
        assertThat(lost).hasValue(10);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void lossCallbackSeesFailedAndPartiallyRejectedDeliveriesOnce() {
        var next = new DeliverySupport.Recording();
        var lost = new AtomicLong();
        var q = queue(policy(8, OverflowPolicy.FAIL), next, lost);
        q.start();

        next.always(DeliveryResult.permanent("down", PROV));
        q.offer(CTX, traces(3));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(lost).hasValue(3);

        next.always(DeliveryResult.partial(2, "two bad", PROV));
        q.offer(CTX, traces(5));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(lost).hasValue(5);

        next.always(DeliveryResult.partial(0, "warning only", PROV));
        q.offer(CTX, traces(4));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(lost).hasValue(5);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void lossCallbackSeesEntriesEvictedByDropOldest() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var lost = new AtomicLong();
        var q = queue(policy(1, OverflowPolicy.DROP_OLDEST), next, lost);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));

        assertThat(lost).hasValue(2);
        gate.countDown();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void lossCallbackSeesEntriesAbandonedByAFatalWorkerError() throws InterruptedException {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        SignalConsumer<TracesData> next = (context, payload) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            throw new AssertionError("fatal downstream failure");
        };
        var lost = new AtomicLong();
        var q = new DeliveryQueue<>(policy(4, OverflowPolicy.FAIL), next, PROV, (items, reason) -> lost.addAndGet(items));
        q.start();

        q.offer(CTX, traces(1));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        q.offer(CTX, traces(2));
        release.countDown();
        q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(lost).hasValue(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void aRefusalAfterShutdownIsReturnedAndRecordedNowhere() {
        var next = new DeliverySupport.Recording();
        var lost = new AtomicLong();
        var q = queue(policy(8, OverflowPolicy.FAIL), next, lost);
        q.start();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();

        var refused = q.offerAll(CTX, List.of(traces(2), traces(3)), () -> {});
        assertThat(refused.result()).isInstanceOfSatisfying(DeliveryResult.Rejected.class, rejected -> {
            assertThat(rejected.reason()).isEqualTo(DeliveryReason.SHUTDOWN);
            assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        });

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        assertThat(lost).hasValue(0);
    }

    @Test
    void anEffectivelyUnboundedCapacityNeverRefusesAGroup() {
        var next = new DeliverySupport.Recording();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(Integer.MAX_VALUE, OverflowPolicy.FAIL), next);
        q.start();

        for (var i = 0; i < 2000; i++) {
            assertThat(q.offerAll(CTX, List.of(traces(1), traces(1)), () -> {}).result())
                    .isInstanceOf(DeliveryResult.Accepted.class);
        }

        gate.countDown();
        assertThat(q.flush(FIVE_SECONDS).toCompletableFuture().join().isSuccess()).isTrue();
        assertThat(next.totalItems()).isEqualTo(4000);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }
}
