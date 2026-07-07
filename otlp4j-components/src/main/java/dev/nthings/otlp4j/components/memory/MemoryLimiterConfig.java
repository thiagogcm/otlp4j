package dev.nthings.otlp4j.components.memory;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import java.time.Duration;

/// The typed configuration for the memory limiter processor, modelled on the Collector's
/// `internal/memorylimiter/config.go`. The soft threshold is `limitBytes - spikeLimitBytes` and the
/// hard threshold is `limitBytes`, so the spike limit is the band between admitting-with-collection and
/// refusing, exactly the Collector's soft-equals-limit-minus-spike and hard-equals-limit model.
///
/// @param checkInterval   how often the used-memory gauge is sampled, strictly positive
/// @param limitBytes      the hard limit in bytes, at or above which ingress is refused, strictly positive
/// @param spikeLimitBytes the spike band in bytes below the hard limit, non-negative and smaller than the limit
/// @param retryAfter      the retry-after hint on a hard-limit refusal, strictly positive
public record MemoryLimiterConfig(
        Duration checkInterval, long limitBytes, long spikeLimitBytes, Duration retryAfter)
        implements ComponentConfig {

    private static final long MIB = 1024L * 1024L;

    /// Validates the check interval, the limit, the spike band, and the retry-after hint against the
    /// same rules the Collector's `Validate` applies.
    public MemoryLimiterConfig {
        if (checkInterval == null || checkInterval.isZero() || checkInterval.isNegative()) {
            throw new IllegalArgumentException("checkInterval must be non-null and strictly positive");
        }
        if (limitBytes <= 0) {
            throw new IllegalArgumentException("limitBytes must be strictly positive, got " + limitBytes);
        }
        if (spikeLimitBytes < 0) {
            throw new IllegalArgumentException("spikeLimitBytes must be non-negative, got " + spikeLimitBytes);
        }
        if (spikeLimitBytes >= limitBytes) {
            throw new IllegalArgumentException(
                    "spikeLimitBytes must be smaller than limitBytes, got spike=" + spikeLimitBytes
                            + " limit=" + limitBytes);
        }
        if (retryAfter == null || retryAfter.isZero() || retryAfter.isNegative()) {
            throw new IllegalArgumentException("retryAfter must be non-null and strictly positive");
        }
    }

    /// The soft threshold in bytes, `limitBytes - spikeLimitBytes`, at or above which the limiter
    /// admits payloads while requesting a best-effort collection.
    ///
    /// @return the soft threshold in bytes
    public long softLimitBytes() {
        return limitBytes - spikeLimitBytes;
    }

    /// The hard threshold in bytes, `limitBytes`, at or above which the limiter refuses ingress.
    ///
    /// @return the hard threshold in bytes
    public long hardLimitBytes() {
        return limitBytes;
    }

    /// The default memory limiter configuration: a one second check interval, a 512 MiB hard limit, a
    /// 128 MiB spike band (a 384 MiB soft threshold), and a one second retry-after hint.
    ///
    /// @return the default configuration
    public static MemoryLimiterConfig defaults() {
        return new MemoryLimiterConfig(Duration.ofSeconds(1), 512 * MIB, 128 * MIB, Duration.ofSeconds(1));
    }
}
