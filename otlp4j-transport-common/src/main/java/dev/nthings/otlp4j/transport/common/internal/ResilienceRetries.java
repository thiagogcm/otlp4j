package dev.nthings.otlp4j.transport.common.internal;

import dev.nthings.otlp4j.core.policy.RetryPolicy;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.core.functions.CheckedSupplier;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/// Shared Resilience4j retry adapter for bundled transports. This is the single place the neutral
/// [RetryPolicy] is translated into Resilience4j. It lives in a non-exported internal package that is
/// qualified-exported only to the bundled gRPC and HTTP transports, so no Resilience4j type is
/// reachable from any public otlp4j API surface.
public final class ResilienceRetries {

    private ResilienceRetries() {}

    /// Translates the neutral [RetryPolicy] into an equivalent Resilience4j [RetryConfig]: the
    /// attempt count carries over directly, and the backoff schedule maps to an exponential random
    /// backoff interval function with the policy's initial delay, multiplier, jitter, and cap.
    public static RetryConfig toRetryConfig(RetryPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        var backoff = policy.backoff();
        return RetryConfig.custom()
                .maxAttempts(policy.maxAttempts())
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        backoff.initial(), backoff.multiplier(), backoff.jitter(), backoff.max()))
                .build();
    }

    /// Builds a retry from the neutral [RetryPolicy] while enforcing the transport's retryable-error
    /// predicate. The server-requested backoff is folded into the interval function only when the
    /// policy opts to respect the server's retry-after hint.
    public static Retry create(
            String name,
            RetryPolicy policy,
            Predicate<Throwable> retryable,
            Function<Throwable, Duration> retryAfter) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(retryAfter, "retryAfter");
        var effectiveRetryAfter =
                policy.respectServerRetryAfter() ? retryAfter : (Function<Throwable, Duration>) t -> Duration.ZERO;
        return create(name, toRetryConfig(policy), retryable, effectiveRetryAfter);
    }

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
