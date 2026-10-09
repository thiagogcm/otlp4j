package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import java.time.Duration;
import java.util.List;

/// The typed configuration for the batch processor: the flush triggers, the bounded delivery queue,
/// and the admission limits that cover buffered, queued, and in-flight submissions together.
///
/// The `sizer` supplies every byte figure this processor uses. A submission the sizer weighs counts
/// toward `maxPendingBytes`; one it weighs as zero counts its items toward `maxPendingItems` instead.
/// The default sizer, [PayloadSizer#reported()], knows only the size a payload already carries, so it
/// weighs a payload received over OTLP and nothing built in process. A hard output-byte cap has to
/// weigh the batches this processor builds itself, so it needs a sizer that can weigh any payload.
///
/// @param batch          the flush-trigger policy: target and maximum sizes, timeout, metadata keys
/// @param delivery       the bounded delivery queue policy behind the flush
/// @param degradedWindow how long the delivery queue refuses ingress after a failed downstream delivery
/// @param maxPendingSubmissions maximum accepted submissions retained by this processor at once
/// @param maxPendingBytes maximum weight, as measured by `sizer`, retained by this processor at once
/// @param maxPartitions maximum metadata partitions retained by this processor at once
/// @param maxPendingItems maximum items retained at once from submissions `sizer` weighs as zero; one
///                       such submission is still admitted alone when it exceeds this limit
/// @param sizer          the source of payload weights for the byte limits and the byte cap
public record BatchProcessorConfig(
        BatchPolicy batch,
        DeliveryQueuePolicy delivery,
        Duration degradedWindow,
        int maxPendingSubmissions,
        long maxPendingBytes,
        int maxPartitions,
        long maxPendingItems,
        PayloadSizer sizer)
        implements ComponentConfig {

    /// Validates the wrapped policies, degraded window, global pending-work limits, and that a hard
    /// byte cap comes with a sizer able to enforce it.
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
        if (maxPendingItems < 1) {
            throw new IllegalArgumentException("maxPendingItems must be >= 1");
        }
        if (sizer == null) {
            throw new IllegalArgumentException("sizer must be non-null; use PayloadSizer.reported()");
        }
        if (batch.sendBatchMaxBytes() > 0 && sizer == PayloadSizer.reported()) {
            throw new IllegalArgumentException(
                    "sendBatchMaxBytes needs a sizer that can weigh every payload, because the default "
                            + "sizer cannot weigh the batches this processor builds; configure an exact "
                            + "sizer such as the transport's OtlpPayloadSizer.exact()");
        }
    }

    /// A configuration with the default pending-item limit and the default sizer, which weighs only
    /// payloads that already carry an encoded size.
    ///
    /// @param batch          the flush-trigger policy, without a hard byte cap
    /// @param delivery       the bounded delivery queue policy behind the flush
    /// @param degradedWindow how long the delivery queue refuses ingress after a failed delivery
    /// @param maxPendingSubmissions maximum accepted submissions retained at once
    /// @param maxPendingBytes maximum weight retained at once from payloads that carry a size
    /// @param maxPartitions maximum metadata partitions retained at once
    public BatchProcessorConfig(
            BatchPolicy batch,
            DeliveryQueuePolicy delivery,
            Duration degradedWindow,
            int maxPendingSubmissions,
            long maxPendingBytes,
            int maxPartitions) {
        this(batch, delivery, degradedWindow, maxPendingSubmissions, maxPendingBytes, maxPartitions,
                DEFAULT_MAX_PENDING_ITEMS, PayloadSizer.reported());
    }

    public static final int DEFAULT_MAX_PENDING_SUBMISSIONS = 1024;
    public static final long DEFAULT_MAX_PENDING_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_MAX_PARTITIONS = 1024;

    /// The default pending-item limit: what the default 64 MiB byte limit holds at 256 bytes per item.
    /// It is a chosen starting point rather than a measured one, and it bounds a count, not memory.
    public static final long DEFAULT_MAX_PENDING_ITEMS = 262_144L;

    /// The default batch configuration: a one second flush timeout, an 8192-item target with no hard
    /// item or byte cap and no metadata keys, a single-consumer queue of 1024 entries applying
    /// backpressure on overflow without waiting for the downstream result, a five second degraded
    /// window, and the default sizer.
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
