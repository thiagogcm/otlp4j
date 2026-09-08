package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import java.time.Duration;
import java.util.List;

/// The typed configuration for the batch processor: the flush triggers, the bounded delivery queue,
/// and the admission limits that cover buffered, queued, and in-flight submissions together.
///
/// @param batch          the flush-trigger policy: target and maximum sizes, timeout, metadata keys
/// @param delivery       the bounded delivery queue policy behind the flush
/// @param degradedWindow how long the delivery queue refuses ingress after a failed downstream delivery
/// @param maxPendingSubmissions maximum accepted submissions retained by this processor at once
/// @param maxPendingBytes maximum exact serialized weight retained by this processor at once
/// @param maxPartitions maximum metadata partitions retained by this processor at once
public record BatchProcessorConfig(
        BatchPolicy batch,
        DeliveryQueuePolicy delivery,
        Duration degradedWindow,
        int maxPendingSubmissions,
        long maxPendingBytes,
        int maxPartitions)
        implements ComponentConfig {

    /// Validates the wrapped policies, degraded window, and global pending-work limits.
    public BatchProcessorConfig {
        if (batch == null) {
            throw new IllegalArgumentException("batch must be non-null");
        }
        if (delivery == null) {
            throw new IllegalArgumentException("delivery must be non-null");
        }
        if (degradedWindow == null || degradedWindow.isNegative()) {
            throw new IllegalArgumentException("degradedWindow must be non-null and non-negative");
        }
        if (maxPendingSubmissions < 1) {
            throw new IllegalArgumentException("maxPendingSubmissions must be >= 1");
        }
        if (maxPendingBytes < 1) {
            throw new IllegalArgumentException("maxPendingBytes must be >= 1");
        }
        if (maxPartitions < 1) {
            throw new IllegalArgumentException("maxPartitions must be >= 1");
        }
    }

    public static final int DEFAULT_MAX_PENDING_SUBMISSIONS = 1024;
    public static final long DEFAULT_MAX_PENDING_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_MAX_PARTITIONS = 1024;

    /// The default batch configuration: a one second flush timeout, an 8192-item target with no hard
    /// item or byte cap and no metadata keys, a single-consumer queue of 1024 entries applying
    /// backpressure on overflow without waiting for the downstream result, and a five second degraded
    /// window.
    ///
    /// @return the default configuration
    public static BatchProcessorConfig defaults() {
        return new BatchProcessorConfig(
                new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5),
                DEFAULT_MAX_PENDING_SUBMISSIONS,
                DEFAULT_MAX_PENDING_BYTES,
                DEFAULT_MAX_PARTITIONS);
    }
}
