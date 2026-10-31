package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import java.time.Duration;
import java.util.List;

/// The typed configuration for the batch processor: the flush triggers, the number of delivery
/// workers, and the admission limits that cover buffered, queued, and in-flight submissions together.
///
/// Those admission limits are the only bound on accepted work. The queue behind the flush has no
/// capacity of its own that could refuse a flush after its submissions were acknowledged.
///
/// The `sizer` supplies every byte figure this processor uses. A submission the sizer weighs counts
/// toward `maxPendingBytes`; one it weighs as zero counts its items toward `maxPendingItems` instead.
/// The default sizer, [PayloadSizer#reported()], knows only the size a payload already carries, so it
/// weighs a payload received over OTLP and nothing built in process. A hard output-byte cap has to
/// weigh the batches this processor builds itself, so it needs a sizer that can weigh any payload.
///
/// @param batch          the flush-trigger policy: target and maximum sizes, timeout, metadata keys
/// @param numConsumers   the number of workers delivering flushed batches downstream, at least one
/// @param maxPendingSubmissions maximum accepted submissions retained by this processor at once
/// @param maxPendingBytes maximum weight, as measured by `sizer`, retained by this processor at once
/// @param maxPartitions maximum metadata partitions retained by this processor at once
/// @param maxPendingItems maximum items retained at once from submissions `sizer` weighs as zero; one
///                       such submission is still admitted alone when it exceeds this limit
/// @param sizer          the source of payload weights for the byte limits and the byte cap
public record BatchProcessorConfig(
        BatchPolicy batch,
        int numConsumers,
        int maxPendingSubmissions,
        long maxPendingBytes,
        int maxPartitions,
        long maxPendingItems,
        PayloadSizer sizer)
        implements ComponentConfig {

    /// Validates the wrapped policy, consumer count, global pending-work limits, and that a hard
    /// byte cap comes with a sizer able to enforce it.
    public BatchProcessorConfig {
        if (batch == null) {
            throw new IllegalArgumentException("batch must be non-null");
        }
        if (numConsumers < 1) {
            throw new IllegalArgumentException("numConsumers must be >= 1, got " + numConsumers);
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
    /// @param numConsumers   the number of workers delivering flushed batches downstream
    /// @param maxPendingSubmissions maximum accepted submissions retained at once
    /// @param maxPendingBytes maximum weight retained at once from payloads that carry a size
    /// @param maxPartitions maximum metadata partitions retained at once
    public BatchProcessorConfig(
            BatchPolicy batch,
            int numConsumers,
            int maxPendingSubmissions,
            long maxPendingBytes,
            int maxPartitions) {
        this(batch, numConsumers, maxPendingSubmissions, maxPendingBytes, maxPartitions,
                DEFAULT_MAX_PENDING_ITEMS, PayloadSizer.reported());
    }

    public static final int DEFAULT_MAX_PENDING_SUBMISSIONS = 1024;

    /// The default number of delivery workers.
    public static final int DEFAULT_NUM_CONSUMERS = 1;

    public static final long DEFAULT_MAX_PENDING_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_MAX_PARTITIONS = 1024;

    /// The default pending-item limit: what the default 64 MiB byte limit holds at 256 bytes per item.
    /// It is a chosen starting point rather than a measured one, and it bounds a count, not memory.
    public static final long DEFAULT_MAX_PENDING_ITEMS = 262_144L;

    /// The default batch configuration: a one second flush timeout, an 8192-item target with no hard
    /// item or byte cap and no metadata keys, a single delivery worker, and the default sizer.
    ///
    /// @return the default configuration
    public static BatchProcessorConfig defaults() {
        return new BatchProcessorConfig(
                new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of()),
                DEFAULT_NUM_CONSUMERS,
                DEFAULT_MAX_PENDING_SUBMISSIONS,
                DEFAULT_MAX_PENDING_BYTES,
                DEFAULT_MAX_PARTITIONS);
    }
}
