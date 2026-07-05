package dev.nthings.otlp4j.pipeline.ops;

/// When a [Batcher] completes the ack it returns to its caller.
///
///   - `ON_DELIVERY` (the default) - the ack stays open until the batch has been drained and
///     delivered downstream, and reflects that downstream [dev.nthings.otlp4j.model.ConsumeResult].
///     A batch merged with others shares their delivery fate. This is the recommended mode for a
///     forwarding gateway: the OTLP request is held (bounded by [Batcher.Builder#flushInterval] to
///     first drain, and by [Batcher.Builder#deliveryTimeout] in flight) until the data is really
///     forwarded, so a sender learns of a downstream failure and retains its data. It is the
///     delivery-ack the OpenTelemetry Collector spells `wait_for_result: true`.
///   - `ON_ENQUEUE` - the ack completes `Accepted` the moment the batch is queued, before delivery.
///     A drain that later fails downstream loses the batch with no upstream signal (surfaced only
///     through [Batcher#drainFailedCount] and the WARN log). Lossy by design; use it for
///     high-throughput, tap-like paths that value ingress latency over delivery guarantees.
public enum AckMode {

    /// Ack after the batch is drained and delivered downstream, reflecting the real result.
    ON_DELIVERY,

    /// Ack `Accepted` at enqueue, before delivery. Lossy on a later drain failure.
    ON_ENQUEUE
}
