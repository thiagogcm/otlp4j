package dev.nthings.otlp4j.core.result;

/// The cause of a delivery result, as a reason code. Admission refusal, timeout, and shutdown are
/// reason codes here rather than distinct result types, which keeps the whole-type taxonomy to
/// permanent, retryable, and downstream. The downstream concept is carried by the boolean on a
/// rejection, never duplicated as a reason code, so a downstream refusal and a downstream
/// unavailability differ only by disposition and retry-after, not by reason.
public enum DeliveryReason {

    /// A failure local to the producing component (a policy fault, a validation error, an
    /// unreachable local resource).
    LOCAL_FAILURE,

    /// The receiver refused admission because it is at capacity; retryable, often with a hint.
    ADMISSION_REFUSED,

    /// The operation did not complete within its deadline.
    TIMEOUT,

    /// The gateway is shutting down and cannot accept the payload.
    SHUTDOWN,

    /// A failure translating between protocol representations.
    PROTOCOL_TRANSLATION,

    /// No consumer is wired for the payload's signal.
    NO_CONSUMER
}
