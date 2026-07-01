package dev.nthings.otlp4j.processor;

/// Policy for an item that arrives while a bounded buffer is full - the batching processor's queue
/// or a telemetry tap subscription's buffer.
///
/// The constants are site-neutral; their concrete effect differs by where the policy is applied:
///
///   - `DROP_OLDEST` - evict the oldest buffered item to make room for the new one.
///   - `DROP_NEWEST` - drop the arriving item. The batcher reports a partial-success rejection for
///     its items; the tap increments its drop counter.
///   - `BLOCK` - block until capacity frees up. The batcher blocks the calling consume stage; the
///     tap blocks the receiver's publish thread (risky in production - a slow tap then
///     back-pressures the in-pipeline path).
///   - `FAIL` - fail the operation. The batcher rejects the batch retryably; the tap terminates the
///     subscription with onError.
public enum OverflowPolicy {

    DROP_OLDEST,

    DROP_NEWEST,

    BLOCK,

    FAIL
}
