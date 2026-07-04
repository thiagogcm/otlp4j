package dev.nthings.otlp4j.pipeline.ops;

/// Policy for a batch that arrives while the [Batcher]'s bounded queue is full.
/// (Telemetry tap subscriptions have their own, non-blocking policy:
/// `dev.nthings.otlp4j.receiver.TapOptions.Overflow`.)
///
///   - `DROP_OLDEST` - evict the oldest queued batch to make room for the new one.
///   - `DROP_NEWEST` - drop the arriving batch, reporting a partial-success rejection for its
///     items.
///   - `BLOCK` - block the calling consume stage until capacity frees up: explicit, opt-in
///     ingress backpressure the sender observes as latency.
///   - `FAIL` - reject the batch retryably with a retry-after hint of one flush interval.
public enum OverflowPolicy {

    DROP_OLDEST,

    DROP_NEWEST,

    BLOCK,

    FAIL
}
