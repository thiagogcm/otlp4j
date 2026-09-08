package dev.nthings.otlp4j.core.policy;

/// The policy for a bounded delivery queue in front of a consumer: the queue capacity, the number of
/// draining consumers, and the overflow behaviour when the queue is full. Admission is always
/// acknowledged at the first bounded buffer; downstream outcomes are surfaced by the component flush
/// report rather than by an optional result-waiting mode.
///
/// @param queueSize     the queue capacity in entries, at least one
/// @param numConsumers  the number of draining consumers, at least one
/// @param overflow      the behaviour when the queue is full
public record DeliveryQueuePolicy(int queueSize, int numConsumers, OverflowPolicy overflow) {

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
