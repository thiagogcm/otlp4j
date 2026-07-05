package dev.nthings.otlp4j.pipeline.ops;

/// Policy for what happens when a bounded buffer is full. Shared by two buffering sites with
/// slightly different mechanics, so each value is documented for both:
///
///   - the [Batcher]'s queue, which buffers whole batches on the acknowledgement path, and
///   - a telemetry tap subscription (`dev.nthings.otlp4j.receiver.TapOptions`), which buffers
///     individual items outside the acknowledgement path.
///
/// The values:
///
///   - `DROP_OLDEST` - For the Batcher: evict the oldest queued batch to admit the new one. For the
///     tap: drop the oldest buffered item so the newest is delivered.
///   - `DROP_NEWEST` - For the Batcher: drop the arriving batch, reporting a partial-success
///     rejection for its items. For the tap: drop the arriving item; the subscriber never sees it.
///   - `BLOCK` - Batcher-only: apply backpressure until space frees (explicit, opt-in ingress
///     backpressure the sender observes as latency). Rejected by the tap, which sits outside the
///     acknowledgement path and must never delay the OTLP response.
///   - `FAIL` - For the Batcher: reject the batch retryably with a retry-after hint of one flush
///     interval. For the tap: terminate the subscription with `onError`.
public enum OverflowPolicy {

    DROP_OLDEST,

    DROP_NEWEST,

    BLOCK,

    FAIL
}
