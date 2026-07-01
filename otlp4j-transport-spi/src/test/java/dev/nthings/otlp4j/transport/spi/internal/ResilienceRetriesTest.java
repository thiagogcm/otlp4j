package dev.nthings.otlp4j.transport.spi.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ResilienceRetries")
class ResilienceRetriesTest {

    @DisplayName("retries transport-retryable exceptions through Resilience4j")
    @Test
    void retriesTransportRetryableExceptions() {
        var attempts = new AtomicInteger();
        var retry = ResilienceRetries.create(
                "test",
                RetryConfig.custom().maxAttempts(3).waitDuration(Duration.ofMillis(1)).build(),
                Retryable.class::isInstance,
                _ -> Duration.ZERO);

        var result = ResilienceRetries.execute(retry, () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new Retryable();
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @DisplayName("does not retry non-retryable transport exceptions")
    @Test
    void doesNotRetryNonRetryableExceptions() {
        var attempts = new AtomicInteger();
        var retry = ResilienceRetries.create(
                "test",
                RetryConfig.custom().maxAttempts(3).waitDuration(Duration.ofMillis(1)).build(),
                Retryable.class::isInstance,
                _ -> Duration.ZERO);

        assertThatThrownBy(() -> ResilienceRetries.execute(retry, () -> {
                    attempts.incrementAndGet();
                    throw new Permanent();
                }))
                .isInstanceOf(Permanent.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    @DisplayName("keeps caller exception predicate and server retry-after delay")
    @Test
    void keepsConfiguredPredicateAndRetryAfterDelay() {
        var waitInterval = new AtomicReference<Duration>();
        var retry = ResilienceRetries.create(
                "test",
                RetryConfig.custom()
                        .maxAttempts(2)
                        .waitDuration(Duration.ofMillis(1))
                        .retryOnException(t -> !(t instanceof CallerIgnored))
                        .build(),
                RuntimeException.class::isInstance,
                _ -> Duration.ofMillis(25));
        retry.getEventPublisher().onRetry(event -> waitInterval.set(event.getWaitInterval()));

        assertThatThrownBy(() -> ResilienceRetries.execute(retry, () -> {
                    throw new CallerIgnored();
                }))
                .isInstanceOf(CallerIgnored.class);

        var attempts = new AtomicInteger();
        var result = ResilienceRetries.execute(retry, () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new Retryable();
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(waitInterval.get()).isEqualTo(Duration.ofMillis(25));
    }

    private static final class Retryable extends RuntimeException {}

    private static final class Permanent extends RuntimeException {}

    private static final class CallerIgnored extends RuntimeException {}
}
