package dev.nthings.otlp4j.core.policy;

/// What happens when a bounded buffer is full.
///
///   - `DROP_OLDEST` evicts the oldest buffered entry to admit the new one.
///   - `DROP_NEWEST` drops the arriving entry.
///   - `FAIL` rejects the arriving entry retryably, typically with a retry-after hint.
public enum OverflowPolicy {

    /// Evict the oldest buffered entry to admit the new one.
    DROP_OLDEST,

    /// Drop the arriving entry.
    DROP_NEWEST,

    /// Reject the arriving entry retryably.
    FAIL
}
