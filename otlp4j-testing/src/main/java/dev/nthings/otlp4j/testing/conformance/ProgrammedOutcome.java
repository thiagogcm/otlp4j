package dev.nthings.otlp4j.testing.conformance;

import java.time.Duration;

/// The result a scenario programs the gateway's per-signal consumer to produce, expressed
/// without any otlp4j result type.
///
/// The seam implementation in otlp4j-conformance translates each case to whatever the current
/// runtime uses. Today that is the fluent runtime's per-batch result; after the Phase 1 API break
/// it is the neutral delivery result. The suite never notices, because it only ever asserts the
/// wire status the gateway emits for a given outcome.
public sealed interface ProgrammedOutcome
        permits ProgrammedOutcome.Accepted, ProgrammedOutcome.Partial, ProgrammedOutcome.Retryable,
        ProgrammedOutcome.Throttled, ProgrammedOutcome.Permanent, ProgrammedOutcome.ThrowRuntime {

    /// The whole batch was accepted. Maps to HTTP `200` with no partial success, and to gRPC `OK`.
    record Accepted() implements ProgrammedOutcome {}

    /// The batch was accepted but some items were rejected. Maps to HTTP `200` and gRPC `OK`, each
    /// carrying a partial success with the rejected count.
    ///
    /// @param rejectedItems the number of items the consumer reports as rejected
    /// @param message the human-readable partial success message
    record Partial(long rejectedItems, String message) implements ProgrammedOutcome {}

    /// A transient rejection the client may retry. Maps to HTTP `503` without a `Retry-After`, and
    /// to gRPC `UNAVAILABLE` without a pushback trailer.
    ///
    /// @param message the rejection message
    record Retryable(String message) implements ProgrammedOutcome {}

    /// A transient rejection carrying an explicit backoff hint. Maps to HTTP `503` with a
    /// `Retry-After` in whole seconds, and to gRPC `UNAVAILABLE` with a `grpc-retry-pushback-ms`
    /// trailer.
    ///
    /// @param retryAfter the requested client backoff
    /// @param message the rejection message
    record Throttled(Duration retryAfter, String message) implements ProgrammedOutcome {}

    /// A permanent rejection the client should not retry. Maps to HTTP `500` and gRPC `INTERNAL`.
    ///
    /// @param message the rejection message
    record Permanent(String message) implements ProgrammedOutcome {}

    /// The consumer throws a runtime exception rather than returning a result. Maps to HTTP `500`
    /// and gRPC `INTERNAL`, proving the dispatcher's failure path.
    ///
    /// @param message the thrown exception message
    record ThrowRuntime(String message) implements ProgrammedOutcome {}
}
