package dev.nthings.otlp4j.core.policy;

/// What happens when a bounded buffer is full, the neutral relocation of the legacy overflow policy
/// with the same four values.
///
///   - `DROP_OLDEST` evicts the oldest buffered entry to admit the new one.
///   - `DROP_NEWEST` drops the arriving entry.
///   - `BLOCK` applies backpressure until space frees, which the sender observes as latency.
///   - `FAIL` rejects the arriving entry retryably, typically with a retry-after hint.
public enum OverflowPolicy {

    /// Evict the oldest buffered entry to admit the new one.
    DROP_OLDEST,

    /// Drop the arriving entry.
    DROP_NEWEST,

    /// Apply backpressure until space frees.
    BLOCK,

    /// Reject the arriving entry retryably.
    FAIL
}
