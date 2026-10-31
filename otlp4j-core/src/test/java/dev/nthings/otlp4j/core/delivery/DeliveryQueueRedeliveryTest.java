package dev.nthings.otlp4j.core.delivery;

import static dev.nthings.otlp4j.core.delivery.DeliverySupport.CTX;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.FIVE_SECONDS;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.PROV;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/// Redelivery: which outcomes are retried, how long the queue waits, when it gives up, and what
/// shutdown does to an entry that is waiting. Backoffs are milliseconds so the tests stay fast; jitter
/// is zero wherever a test reads the timing.
class DeliveryQueueRedeliveryTest {

    private final List<DeliveryQueue<TracesData>> started = new CopyOnWriteArrayList<>();
    private final List<String> reasons = new CopyOnWriteArrayList<>();
    private final AtomicLong lostItems = new AtomicLong();

    private static RedeliveryPolicy redelivery(long initialMillis, long maxMillis, long elapsedMillis) {
        return new RedeliveryPolicy(
                new BackoffPolicy(Duration.ofMillis(initialMillis), 1.0, 0.0, Duration.ofMillis(maxMillis)),
                Duration.ofMillis(elapsedMillis));
    }

    private DeliveryQueue<TracesData> started(
            int consumers, RedeliveryPolicy redelivery, SignalConsumer<TracesData> next) {
        var q = new DeliveryQueue<>(
                new DeliveryQueuePolicy(16, consumers, OverflowPolicy.FAIL), redelivery, next, PROV,
                (items, reason) -> {
                    lostItems.addAndGet(items);
                    reasons.add(reason);
                });
        q.start();
        started.add(q);
        return q;
    }

    @AfterEach
    void stopQueues() {
        for (var q : started) {
            q.drainAndStop(Duration.ofSeconds(2));
        }
    }

    @Test
    void aRetryableFailureIsRedeliveredUntilTheDestinationRecovers() {
        var next = new DeliverySupport.Recording();
        next.byAttempt(attempt -> attempt < 4
                ? DeliveryResult.retryable("down", PROV)
                : DeliveryResult.accepted(PROV));
        var q = started(1, redelivery(5, 5, 5_000), next);

        assertThat(q.offer(CTX, traces(3)).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(next.received()).hasSize(4);
        assertThat(next.received()).allSatisfy(p -> assertThat(p).isSameAs(next.received().getFirst()));
        assertThat(report.isSuccess()).isTrue();
        assertThat(lostItems).hasValue(0);
    }

    @Test
    void aPermanentRejectionIsNotRetried() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.permanent("bad request", PROV));
        var q = started(1, redelivery(5, 5, 5_000), next);

        q.offer(CTX, traces(2));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(next.received()).hasSize(1);
        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(lostItems).hasValue(2);
        assertThat(reasons).singleElement().asString().contains("downstream rejected the delivery");
    }

    @Test
    void aPartialSuccessIsNeverRetried() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.partial(1, "one bad span", PROV));
        var q = started(1, redelivery(5, 5, 5_000), next);

        q.offer(CTX, traces(3));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(next.received()).hasSize(1);
        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(lostItems).hasValue(1);
    }

    @Test
    void anEntryThatOutlastsTheBudgetIsLostOnceInTheReportAndTheListener() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.retryable("still down", PROV));
        var q = started(1, redelivery(10, 10, 120), next);

        q.offer(CTX, traces(4));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(next.received().size()).isBetween(3, 14);
        assertThat(report.rejectedItems()).isEqualTo(4);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(lostItems).hasValue(4);
        assertThat(reasons).singleElement().asString().contains("redelivery budget").contains("still down");
    }

    @Test
    void aRetryAfterHintLongerThanTheBackoffIsHonoured() {
        var next = new DeliverySupport.Recording();
        next.byAttempt(attempt -> attempt == 1
                ? DeliveryResult.throttled(Duration.ofMillis(250), "slow down", PROV)
                : DeliveryResult.accepted(PROV));
        var q = started(1, redelivery(1, 1, 5_000), next);

        q.offer(CTX, traces(1));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();

        var times = next.attemptTimes();
        assertThat(times).hasSize(2);
        assertThat(Duration.ofNanos(times.get(1) - times.get(0))).isGreaterThanOrEqualTo(Duration.ofMillis(245));
    }

    @Test
    void aHintThatWouldPassTheBudgetGivesUpAtOnceInsteadOfSleepingThroughIt() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.throttled(Duration.ofHours(1), "come back tomorrow", PROV));
        var q = started(1, redelivery(1, 1, 300), next);

        q.offer(CTX, traces(2));
        var begun = System.nanoTime();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(2));
        assertThat(next.received()).hasSize(1);
        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(lostItems).hasValue(2);
    }

    @Test
    void theWaitGrowsByTheMultiplierAndIsCappedAtTheMaximum() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.retryable("down", PROV));
        var policy = new RedeliveryPolicy(
                new BackoffPolicy(Duration.ofMillis(40), 2.0, 0.0, Duration.ofMillis(80)), Duration.ofMillis(400));
        var q = started(1, policy, next);

        q.offer(CTX, traces(1));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();

        // Waits of 40, 80, 80, ...: the third wait is capped rather than 160.
        var times = next.attemptTimes();
        assertThat(times.size()).isGreaterThanOrEqualTo(4);
        assertThat(Duration.ofNanos(times.get(1) - times.get(0))).isBetween(Duration.ofMillis(35), Duration.ofMillis(75));
        assertThat(Duration.ofNanos(times.get(3) - times.get(2))).isBetween(Duration.ofMillis(75), Duration.ofMillis(150));
    }

    @Test
    void jitterNeverPushesAWaitBeyondTheCap() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.retryable("down", PROV));
        var policy = new RedeliveryPolicy(
                new BackoffPolicy(Duration.ofMillis(20), 1.0, 1.0, Duration.ofMillis(20)), Duration.ofMillis(400));
        var q = started(1, policy, next);

        q.offer(CTX, traces(1));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();

        var times = next.attemptTimes();
        assertThat(times.size()).isGreaterThan(3);
        for (var i = 1; i < times.size(); i++) {
            assertThat(Duration.ofNanos(times.get(i) - times.get(i - 1))).isLessThan(Duration.ofMillis(120));
        }
    }

    @Test
    void aRetryingEntryKeepsItsWorkerSoConcurrencyStaysBoundedByTheConsumerCount() {
        var order = new CopyOnWriteArrayList<Long>();
        var failuresLeft = new AtomicInteger(2);
        SignalConsumer<TracesData> next = (context, payload) -> {
            order.add(payload.itemCount());
            var failing = payload.itemCount() == 1 && failuresLeft.getAndDecrement() > 0;
            return CompletableFuture.completedFuture(
                    failing ? DeliveryResult.retryable("down", PROV) : DeliveryResult.accepted(PROV));
        };
        var q = started(1, redelivery(20, 20, 5_000), next);

        q.offer(CTX, traces(1));
        q.offer(CTX, traces(2));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();

        // With one consumer the second entry waits behind the first entry's whole redelivery.
        assertThat(order).containsExactly(1L, 1L, 1L, 2L);
    }

    @Test
    void aFlushWaitsForARetryingEntryUpToItsDeadline() {
        var healthy = new java.util.concurrent.atomic.AtomicBoolean();
        var attempts = new AtomicInteger();
        SignalConsumer<TracesData> next = (context, payload) -> {
            attempts.incrementAndGet();
            return CompletableFuture.completedFuture(
                    healthy.get() ? DeliveryResult.accepted(PROV) : DeliveryResult.retryable("down", PROV));
        };
        var q = started(1, redelivery(10, 10, 60_000), next);

        q.offer(CTX, traces(1));
        assertThatThrownBy(() -> q.flush(Duration.ofMillis(150)).toCompletableFuture().join())
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(attempts.get()).isGreaterThan(2);

        healthy.set(true);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        assertThat(lostItems).hasValue(0);
    }

    @Test
    void shutdownGivesTheWaitingEntryAndEachQueuedEntryOneAttemptEach() {
        var attemptsByItems = new java.util.concurrent.ConcurrentHashMap<Long, AtomicInteger>();
        SignalConsumer<TracesData> next = (context, payload) -> {
            attemptsByItems.computeIfAbsent(payload.itemCount(), k -> new AtomicInteger()).incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.retryable("down", PROV));
        };
        // A wait far longer than the test: only shutdown can end it.
        var q = started(1, redelivery(60_000, 60_000, 600_000), next);

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> attemptsByItems.containsKey(1L));
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));
        var begun = System.nanoTime();
        var report = q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(3));
        assertThat(attemptsByItems.get(1L)).hasValue(2);
        assertThat(attemptsByItems.get(2L)).hasValue(1);
        assertThat(attemptsByItems.get(3L)).hasValue(1);
        assertThat(report.rejectedItems()).isEqualTo(6);
        assertThat(report.failedBatches()).isEqualTo(3);
        assertThat(lostItems).hasValue(6);
        assertThat(reasons).hasSize(3).allSatisfy(r -> assertThat(r).contains("shutdown"));
    }

    @Test
    void shutdownFinishesWithinItsBudgetEvenWhenTheDestinationNeverAnswers() throws Exception {
        var entered = new CountDownLatch(1);
        var never = new CompletableFuture<DeliveryResult>();
        SignalConsumer<TracesData> next = (context, payload) -> {
            entered.countDown();
            return never;
        };
        var q = started(1, redelivery(10, 10, 600_000), next);

        q.offer(CTX, traces(1));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(4));
        var begun = System.nanoTime();
        try {
            assertThatThrownBy(() -> q.drainAndStop(Duration.ofMillis(200)).toCompletableFuture().join())
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(3));
            // The two entries no worker will ever take are lost, and said to be.
            assertThat(lostItems).hasValue(6);
            assertThat(reasons).singleElement().asString().contains("abandoned at shutdown");
        } finally {
            never.complete(DeliveryResult.accepted(PROV));
        }
    }

    @Test
    void aFatalWorkerErrorWakesAnEntryWaitingOutItsBackoff() {
        var attemptsAtOne = new AtomicInteger();
        SignalConsumer<TracesData> next = (context, payload) -> {
            if (payload.itemCount() == 2) {
                throw new AssertionError("fatal downstream failure");
            }
            attemptsAtOne.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.retryable("down", PROV));
        };
        var q = started(2, redelivery(60_000, 60_000, 600_000), next);

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> attemptsAtOne.get() == 1);
        q.offer(CTX, traces(2));
        var begun = System.nanoTime();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(3));
        assertThat(attemptsAtOne).hasValue(2);
        assertThat(report.rejectedItems()).isEqualTo(3);
    }

    @Test
    void withoutARedeliveryPolicyAnEntryGetsOneAttempt() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.retryable("down", PROV));
        var q = new DeliveryQueue<>(
                new DeliveryQueuePolicy(4, 1, OverflowPolicy.FAIL), next, PROV, (items, reason) -> {});
        q.start();
        started.add(q);

        q.offer(CTX, traces(1));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(next.received()).hasSize(1);
        assertThat(report.rejectedItems()).isEqualTo(1);
    }

    @Test
    void sizeCountsEntriesWaitingBeingDeliveredAndBeingRedelivered() {
        var next = new DeliverySupport.Recording();
        next.always(DeliveryResult.retryable("down", PROV));
        var q = started(1, redelivery(60_000, 60_000, 600_000), next);
        assertThat(q.size()).isZero();

        q.offer(CTX, traces(1));
        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);

        assertThat(q.size()).isEqualTo(2);
    }

    @Test
    void anOutageLongerThanAnyOneAttemptDoesNotDuplicateTheDelivery() {
        var accepted = new AtomicInteger();
        var deadUntil = System.nanoTime() + Duration.ofMillis(200).toNanos();
        SignalConsumer<TracesData> next = (ConsumeContext context, Payload<TracesData> payload) -> {
            CompletionStage<DeliveryResult> stage;
            if (System.nanoTime() < deadUntil) {
                stage = CompletableFuture.completedFuture(DeliveryResult.retryable("outage", PROV));
            } else {
                accepted.incrementAndGet();
                stage = CompletableFuture.completedFuture(DeliveryResult.accepted(PROV));
            }
            return stage;
        };
        var q = started(2, redelivery(10, 20, 5_000), next);

        for (var i = 0; i < 5; i++) {
            q.offer(CTX, traces(1));
        }
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.isSuccess()).isTrue();
        assertThat(accepted).hasValue(5);
    }
}
