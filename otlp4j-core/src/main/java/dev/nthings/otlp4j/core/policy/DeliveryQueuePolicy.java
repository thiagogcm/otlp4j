package dev.nthings.otlp4j.core.policy;

/// The policy for a bounded delivery queue in front of a consumer: the queue capacity, the number of
/// draining consumers, the overflow behaviour when the queue is full, and whether the producer waits
/// for the downstream delivery result. The `waitForResult` flag subsumes the legacy acknowledgement
/// mode: when true, the producer's acknowledgement reflects the real downstream result; when false,
/// the producer is acknowledged at enqueue and a later drain failure is not signalled upstream.
///
/// @param queueSize     the queue capacity in entries, at least one
/// @param numConsumers  the number of draining consumers, at least one
/// @param overflow      the behaviour when the queue is full
/// @param waitForResult whether the producer waits for the downstream delivery result
public record DeliveryQueuePolicy(int queueSize, int numConsumers, OverflowPolicy overflow, boolean waitForResult) {

    /// Validates the sizes and overflow policy.
    public DeliveryQueuePolicy {
        if (queueSize < 1) {
            throw new IllegalArgumentException("queueSize must be >= 1, got " + queueSize);
        }
        if (numConsumers < 1) {
            throw new IllegalArgumentException("numConsumers must be >= 1, got " + numConsumers);
        }
        if (overflow == null) {
            throw new IllegalArgumentException("overflow must be non-null");
        }
    }
}
