package dev.nthings.otlp4j.components.memory;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.internal.MemoryLimiter;
import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MemoryLimiterProcessorTest {

    private static final ConsumeContext CTX = new TestConsumeContext();
    private static final ShutdownContext SHUTDOWN = new ShutdownContext(Duration.ofSeconds(5));
    private static final Duration CHECK_INTERVAL = Duration.ofNanos(100);
    private static final Duration RETRY_AFTER = Duration.ofSeconds(2);

    private final AtomicLong used = new AtomicLong();
    private final AtomicLong nanos = new AtomicLong(1_000);
    private final AtomicLong collects = new AtomicLong();
    private final MemoryLimiter limiter = new MemoryLimiter(used::get, 100, 200);
    private final RecordingConsumer next = new RecordingConsumer();
    private final MemoryLimiterProcessor processor = new MemoryLimiterProcessor(
            ComponentId.parse("memory_limiter"),
            SignalDescriptor.TRACES,
            limiter,
            CHECK_INTERVAL,
            RETRY_AFTER,
            next,
            nanos::get,
            collects::incrementAndGet);

    @Test
    void normalStateForwardsWithoutCollecting() {
        used.set(50);
        limiter.refresh();

        var result = consume();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).hasSize(1);
        assertThat(collects.get()).isZero();
    }

    @Test
    void hardStateRefusesWithARetryableThrottledResultAndForwardsNothing() {
        used.set(250);
        limiter.refresh();

        var result = consume();

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) result;
        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.retryAfter()).isEqualTo(RETRY_AFTER);
        assertThat(rejected.message()).contains("hard limit");
        assertThat(next.received()).isEmpty();
    }

    @Test
    void softStateForwardsAndCollectsAtMostOncePerCheckInterval() {
        used.set(150);
        limiter.refresh();

        consume();
        consume();
        // Two consumes inside the same interval collect once.
        assertThat(collects.get()).isEqualTo(1);

        // Advancing past the interval admits a second collection.
        nanos.addAndGet(CHECK_INTERVAL.toNanos());
        consume();
        assertThat(collects.get()).isEqualTo(2);

        assertThat(next.received()).hasSize(3);
    }

    @Test
    void recoversAndAdmitsAgainOnceUsedMemoryDropsBackUnderSoft() {
        used.set(250);
        limiter.refresh();
        assertThat(consume()).isInstanceOf(DeliveryResult.Rejected.class);

        used.set(50);
        limiter.refresh();
        assertThat(consume()).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).hasSize(1);
    }

    @Test
    void startPerformsAnInitialSampleSoTheFirstPayloadSeesTheCurrentState() {
        used.set(250);
        processor.start(baseContext()).toCompletableFuture().join();

        assertThat(consume()).isInstanceOf(DeliveryResult.Rejected.class);

        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void refusesAfterShutdownWithARetryableShuttingDownResult() {
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();

        var result = consume();

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) result;
        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.reason()).isEqualTo(DeliveryReason.SHUTDOWN);
        assertThat(next.received()).isEmpty();
    }

    @Test
    void shutdownIsIdempotent() {
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    private DeliveryResult consume() {
        return processor.consume(CTX, traces(1)).toCompletableFuture().join();
    }
}
