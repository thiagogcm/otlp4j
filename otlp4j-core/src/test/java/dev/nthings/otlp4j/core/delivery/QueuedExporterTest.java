package dev.nthings.otlp4j.core.delivery;

import static dev.nthings.otlp4j.core.delivery.DeliverySupport.CTX;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.FIVE_SECONDS;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.PROV;
import static dev.nthings.otlp4j.core.delivery.DeliverySupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class QueuedExporterTest {

    private static final ComponentContext START = new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
    private static final RedeliveryPolicy FAST = new RedeliveryPolicy(
            new BackoffPolicy(Duration.ofMillis(5), 1.0, 0.0, Duration.ofMillis(5)), Duration.ofSeconds(5));

    /// A delegate exporter that records its lifecycle and every delivery, with programmable answers.
    private static final class Fake implements Exporter<TracesData> {
        final List<String> events = new CopyOnWriteArrayList<>();
        final List<ConsumeContext> contexts = new CopyOnWriteArrayList<>();
        final List<Payload<TracesData>> received = new CopyOnWriteArrayList<>();
        volatile Function<Integer, CompletionStage<DeliveryResult>> answer =
                attempt -> CompletableFuture.completedFuture(DeliveryResult.accepted(PROV));
        volatile FlushReport flushReport = FlushReport.empty();
        volatile Duration shutdownBudget;

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
            events.add("consume");
            contexts.add(context);
            received.add(data);
            return answer.apply(received.size());
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            events.add("start");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<FlushReport> flush(FlushContext context) {
            events.add("flush");
            return CompletableFuture.completedFuture(flushReport);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            events.add("shutdown");
            shutdownBudget = context.deadline();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class Recorder implements QueueObserver {
        final AtomicLong lost = new AtomicLong();
        final AtomicLong refused = new AtomicLong();
        final AtomicBoolean registered = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();
        volatile LongSupplier size;

        @Override
        public void lost(long items, String reason) {
            lost.addAndGet(items);
        }

        @Override
        public void refused(long items, DeliveryResult.Rejected refusal) {
            refused.addAndGet(items);
        }

        @Override
        public GatewayMetrics.Registration depth(LongSupplier size) {
            this.size = size;
            registered.set(true);
            return () -> closed.set(true);
        }
    }

    private static QueuedExporter<TracesData> queued(
            Fake delegate, DeliveryQueuePolicy policy, RedeliveryPolicy redelivery, Recorder observer) {
        return new QueuedExporter<>(delegate, policy, redelivery, PROV, observer);
    }

    private static DeliveryQueuePolicy policy(int size, int consumers) {
        return new DeliveryQueuePolicy(size, consumers, OverflowPolicy.FAIL);
    }

    private static <V> V join(CompletionStage<V> stage) {
        return stage.toCompletableFuture().join();
    }

    @Test
    void acknowledgesOnEnqueueAndDeliversWithTheRequestsMetadataButNotItsLifetime() {
        var delegate = new Fake();
        var q = queued(delegate, policy(8, 1), FAST, new Recorder());
        join(q.start(START));
        var metadata = new ClientMetadata(Optional.empty(), Map.of("x-tenant", List.of("a")));
        var expiring = ConsumeContext.of(metadata, Deadline.after(Duration.ofSeconds(30), Clock.systemUTC()));

        assertThat(join(q.consume(expiring, traces(2)))).isInstanceOf(DeliveryResult.Accepted.class);
        join(q.flush(new FlushContext(FIVE_SECONDS)));

        assertThat(delegate.contexts).singleElement().satisfies(delivered -> {
            assertThat(delivered.metadata()).isEqualTo(metadata);
            assertThat(delivered.deadline()).isEqualTo(Deadline.never());
        });
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
    }

    @Test
    void anAcknowledgedPayloadSurvivesAnOutageAndIsDeliveredOnce() {
        var delegate = new Fake();
        delegate.answer = attempt -> CompletableFuture.completedFuture(attempt < 6
                ? DeliveryResult.retryable("outage", PROV)
                : DeliveryResult.accepted(PROV));
        var observer = new Recorder();
        var q = queued(delegate, policy(8, 1), FAST, observer);
        join(q.start(START));

        assertThat(join(q.consume(CTX, traces(3)))).isInstanceOf(DeliveryResult.Accepted.class);
        var report = join(q.flush(new FlushContext(FIVE_SECONDS)));

        assertThat(report.isSuccess()).isTrue();
        assertThat(delegate.received).hasSize(6);
        assertThat(observer.lost).hasValue(0);
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
    }

    @Test
    void startsTheDelegateBeforeTheQueueDeliversAndRegistersTheDepth() {
        var delegate = new Fake();
        var observer = new Recorder();
        var q = queued(delegate, policy(8, 1), FAST, observer);
        q.consume(CTX, traces(1));
        assertThat(delegate.events).doesNotContain("consume");

        join(q.start(START));
        join(q.flush(new FlushContext(FIVE_SECONDS)));

        assertThat(delegate.events).containsExactly("start", "consume", "flush");
        assertThat(observer.registered).isTrue();
        assertThat(observer.size.getAsLong()).isZero();
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
    }

    @Test
    void aFullQueueRefusesTheCallerAndLeavesNoTraceInTheFlushReport() {
        var delegate = new Fake();
        var release = new CompletableFuture<DeliveryResult>();
        delegate.answer = attempt -> release;
        var observer = new Recorder();
        var q = queued(delegate, policy(1, 1), null, observer);
        join(q.start(START));

        q.consume(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> delegate.received.size() == 1);
        q.consume(CTX, traces(1));
        var refusal = join(q.consume(CTX, traces(5)));

        assertThat(refusal).isInstanceOfSatisfying(DeliveryResult.Rejected.class, rejected -> {
            assertThat(rejected.retryAfter()).isNotNull();
            assertThat(rejected.disposition().name()).isEqualTo("RETRYABLE");
        });
        assertThat(observer.refused).hasValue(5);
        assertThat(observer.lost).hasValue(0);
        assertThat(observer.size.getAsLong()).isEqualTo(2);

        release.complete(DeliveryResult.accepted(PROV));
        assertThat(join(q.flush(new FlushContext(FIVE_SECONDS))).isSuccess()).isTrue();
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
    }

    @Test
    void aCancelledOrExpiredRequestIsNotQueued() {
        var delegate = new Fake();
        var q = queued(delegate, policy(8, 1), FAST, new Recorder());
        join(q.start(START));
        var expired = ConsumeContext.of(
                ClientMetadata.empty(), Deadline.after(Duration.ZERO, Clock.systemUTC()));

        assertThat(join(q.consume(expired, traces(1)))).isInstanceOf(DeliveryResult.Rejected.class);
        join(q.flush(new FlushContext(FIVE_SECONDS)));
        assertThat(delegate.received).isEmpty();
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
    }

    @Test
    void flushCombinesTheQueueReportWithTheDelegatesReport() {
        var delegate = new Fake();
        delegate.flushReport = new FlushReport(0, 7, 1, null);
        delegate.answer = attempt -> CompletableFuture.completedFuture(DeliveryResult.partial(2, "two bad", PROV));
        var q = queued(delegate, policy(8, 1), FAST, new Recorder());
        join(q.start(START));

        q.consume(CTX, traces(5));
        var report = join(q.flush(new FlushContext(FIVE_SECONDS)));

        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(report.droppedItems()).isEqualTo(7);
        assertThat(report.failedBatches()).isEqualTo(1);
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
    }

    @Test
    void shutdownDrainsThenReleasesTheDelegateAndClosesTheDepthRegistration() {
        var delegate = new Fake();
        var observer = new Recorder();
        var q = queued(delegate, policy(8, 1), FAST, observer);
        join(q.start(START));
        q.consume(CTX, traces(1));

        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));

        assertThat(delegate.events).containsExactly("start", "consume", "shutdown");
        assertThat(delegate.shutdownBudget).isLessThanOrEqualTo(FIVE_SECONDS);
        assertThat(observer.closed).isTrue();
        // Idempotent: a second shutdown returns the first outcome without touching the delegate again.
        join(q.shutdown(new ShutdownContext(FIVE_SECONDS)));
        assertThat(delegate.events.stream().filter("shutdown"::equals)).hasSize(1);
    }

    @Test
    void shutdownWithADeadDestinationFinishesWithinItsBudgetReleasesTheDelegateAndSurfacesTheLoss() {
        var delegate = new Fake();
        var never = new CompletableFuture<DeliveryResult>();
        delegate.answer = attempt -> never;
        var observer = new Recorder();
        var q = queued(delegate, policy(8, 1), FAST, observer);
        join(q.start(START));
        q.consume(CTX, traces(1));
        q.consume(CTX, traces(2));
        await().atMost(2, TimeUnit.SECONDS).until(() -> delegate.received.size() == 1);

        var begun = System.nanoTime();
        try {
            assertThatThrownBy(() -> join(q.shutdown(new ShutdownContext(Duration.ofMillis(200)))))
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(3));
            assertThat(delegate.events).endsWith("shutdown");
            assertThat(delegate.shutdownBudget).isLessThanOrEqualTo(Duration.ofMillis(200));
            assertThat(observer.lost).hasValue(2);
        } finally {
            never.complete(DeliveryResult.accepted(PROV));
        }
    }

    @Test
    void shutdownSurfacesAcknowledgedDataThatCouldNotBeDelivered() {
        var delegate = new Fake();
        delegate.answer = attempt -> CompletableFuture.completedFuture(DeliveryResult.permanent("rejected", PROV));
        var observer = new Recorder();
        var q = queued(delegate, policy(8, 1), FAST, observer);
        join(q.start(START));
        q.consume(CTX, traces(2));

        assertThatThrownBy(() -> join(q.shutdown(new ShutdownContext(FIVE_SECONDS))))
                .hasMessageContaining("rejected");
        assertThat(delegate.events).endsWith("shutdown");
        assertThat(observer.lost).hasValue(2);
    }

    @Test
    void aDestinationThatNeverAnswersDoesNotStallAnotherDestinationsQueue() throws Exception {
        var dead = new Fake();
        var never = new CompletableFuture<DeliveryResult>();
        dead.answer = attempt -> never;
        var healthy = new Fake();
        var a = queued(dead, policy(64, 2), FAST, new Recorder());
        var b = queued(healthy, policy(64, 2), FAST, new Recorder());
        join(a.start(START));
        join(b.start(START));
        var allDelivered = new CountDownLatch(20);
        healthy.answer = attempt -> {
            allDelivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.accepted(PROV));
        };

        try {
            for (var i = 0; i < 20; i++) {
                // The same payload goes to both, as a fan-out does: both are acknowledged.
                var payload = traces(1);
                assertThat(join(a.consume(CTX, payload))).isInstanceOf(DeliveryResult.Accepted.class);
                assertThat(join(b.consume(CTX, payload))).isInstanceOf(DeliveryResult.Accepted.class);
            }
            assertThat(allDelivered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(healthy.received).hasSize(20);
        } finally {
            never.complete(DeliveryResult.accepted(PROV));
            join(a.shutdown(new ShutdownContext(FIVE_SECONDS)));
            join(b.shutdown(new ShutdownContext(FIVE_SECONDS)));
        }
    }
}
