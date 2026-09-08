package dev.nthings.otlp4j.core.policy;

import java.time.Duration;
import java.util.List;

/// The batching policy for a batch processor: the target and maximum batch sizes by item count and
/// by byte size, the flush timeout, and the metadata keys a batch is partitioned by. The
/// `metadataKeys` list makes metadata-keyed batching expressible, which depends on the receiver
/// capturing client metadata into the consume context.
///
/// @param sendBatchSize     the target batch size in items before a flush
/// @param sendBatchMaxSize  the hard cap on batch size in items, or `0` for no cap
/// @param sendBatchMaxBytes the hard cap on batch size in bytes, or `0` for no cap
/// @param timeout           the maximum time a partial batch waits before flushing
/// @param metadataKeys      the metadata keys a batch is partitioned by, possibly empty
public record BatchPolicy(
        int sendBatchSize,
        int sendBatchMaxSize,
        long sendBatchMaxBytes,
        Duration timeout,
        List<String> metadataKeys) {

    /// Validates the sizes and timeout and defensively copies the metadata keys.
    public BatchPolicy {
        if (sendBatchSize < 1) {
            throw new IllegalArgumentException("sendBatchSize must be >= 1, got " + sendBatchSize);
        }
        if (sendBatchMaxSize < 0) {
            throw new IllegalArgumentException("sendBatchMaxSize must be >= 0, got " + sendBatchMaxSize);
        }
        if (sendBatchMaxSize != 0 && sendBatchMaxSize < sendBatchSize) {
            throw new IllegalArgumentException("sendBatchMaxSize, when set, must be >= sendBatchSize");
        }
        if (sendBatchMaxBytes < 0) {
            throw new IllegalArgumentException("sendBatchMaxBytes must be >= 0, got " + sendBatchMaxBytes);
        }
        if (timeout == null || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-null and non-negative");
        }
        if (metadataKeys == null) {
            throw new IllegalArgumentException("metadataKeys must be non-null; use List.of()");
        }
        metadataKeys = List.copyOf(metadataKeys);
    }
}
