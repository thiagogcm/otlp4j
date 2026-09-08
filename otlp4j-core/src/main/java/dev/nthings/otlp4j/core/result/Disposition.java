package dev.nthings.otlp4j.core.result;

/// The retry axis of a rejection, orthogonal to its cause and to the downstream marker. `RETRYABLE`
/// tells the sender the failure is transient and it SHOULD retry, mapping to gRPC `UNAVAILABLE` /
/// HTTP `503`. `PERMANENT` tells the sender it MUST NOT retry, mapping to gRPC `INTERNAL` / HTTP
/// `500`.
public enum Disposition {

    /// A transient failure the sender SHOULD retry; maps to gRPC `UNAVAILABLE` / HTTP `503`.
    RETRYABLE,

    /// A permanent failure the sender MUST NOT retry; maps to gRPC `INTERNAL` / HTTP `500`.
    PERMANENT
}
