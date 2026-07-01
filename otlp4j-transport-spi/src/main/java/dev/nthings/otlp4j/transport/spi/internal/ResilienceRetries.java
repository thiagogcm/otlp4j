package dev.nthings.otlp4j.transport.spi.internal;

import io.github.resilience4j.core.functions.CheckedSupplier;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/// Shared Resilience4j retry adapter for bundled transports.
public final class ResilienceRetries {

    private ResilienceRetries() {}

    /// Builds a retry from caller configuration while enforcing the transport's retryable-error
    /// predicate and folding server-requested backoff into Resilience4j's interval function.
    public static Retry create(
            String name,
            RetryConfig config,
            Predicate<Throwable> retryable,
            Function<Throwable, Duration> retryAfter) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(retryable, "retryable");
        Objects.requireNonNull(retryAfter, "retryAfter");

        var configuredPredicate = config.getExceptionPredicate();
        var configuredInterval = config.getIntervalBiFunction();
        var effective = RetryConfig.from(config)
                .retryOnException(t -> retryable.test(t) && configuredPredicate.test(t))
                .intervalBiFunction((attempt, either) -> {
                    var delay = configuredInterval.apply(attempt, either);
                    if (either.isLeft()) {
                        var serverDelay = retryAfter.apply(either.getLeft());
                        if (!serverDelay.isNegative() && !serverDelay.isZero()) {
                            delay = Math.max(delay, serverDelay.toMillis());
                        }
                    }
                    return delay;
                })
                .build();
        return Retry.of(name, effective);
    }

    /// Executes a synchronous transport attempt through Resilience4j.
    public static <T> T execute(Retry retry, CheckedSupplier<T> supplier) {
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(supplier, "supplier");
        try {
            return retry.executeCheckedSupplier(supplier);
        } catch (RuntimeException e) {
            throw e;
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("checked exception escaped retry supplier", e);
        }
    }
}
