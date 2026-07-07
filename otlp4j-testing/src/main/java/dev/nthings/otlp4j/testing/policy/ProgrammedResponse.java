package dev.nthings.otlp4j.testing.policy;

import java.time.Duration;

/// A neutral server behaviour the programmed OTLP server replays for one received attempt. The
/// programmed server holds a `List<ProgrammedResponse>` and answers the n-th received attempt with the
/// n-th element, so a script is a per-attempt plan the exporter under test drives one request at a time
/// against. Each variant names a wire behaviour both legacy transports already read, expressed once so
/// the same script runs unchanged on gRPC and HTTP.
///
/// The variants map to the wire as follows. [Ok] is a full success. [Unavailable] is a retryable
/// rejection with no server hint (gRPC `UNAVAILABLE`, HTTP `503`). [Throttle] is a retryable rejection
/// carrying a numeric server backoff hint (gRPC `grpc-retry-pushback-ms` trailer, HTTP numeric
/// `Retry-After` header). [ThrottleDate] is the same retryable throttle whose HTTP form carries an
/// absolute RFC 1123 date `Retry-After` header, so the date branch of the client parser is exercised;
/// it is HTTP-specific. [ThrottleNoRetry] tells the client to stop retrying (gRPC negative pushback,
/// HTTP a permanent status). [RetryInfo] carries a gRPC `grpc-status-details-bin` RetryInfo detail, with
/// HTTP `Retry-After` as its cross-protocol analog. [Permanent] is a non-retryable rejection. [Hang]
/// holds the attempt open for the given duration before answering, to trigger the per-attempt deadline.
public sealed interface ProgrammedResponse {

    /// A full success. The server answers with an empty OTLP export response.
    record Ok() implements ProgrammedResponse {}

    /// A retryable rejection with no server backoff hint.
    record Unavailable() implements ProgrammedResponse {}

    /// A retryable rejection carrying a numeric server backoff hint.
    ///
    /// @param retryAfter the server-requested backoff before the next attempt
    record Throttle(Duration retryAfter) implements ProgrammedResponse {}

    /// A retryable throttle whose HTTP form carries an absolute-date `Retry-After` header, exercising
    /// the RFC 1123 date branch of the client parser. HTTP-specific.
    ///
    /// @param retryAfter the server-requested backoff before the next attempt
    record ThrottleDate(Duration retryAfter) implements ProgrammedResponse {}

    /// A rejection that tells the client to stop retrying: gRPC negative pushback, HTTP a permanent
    /// status.
    record ThrottleNoRetry() implements ProgrammedResponse {}

    /// A retryable rejection carrying a gRPC RetryInfo detail (HTTP emits the equivalent `Retry-After`).
    ///
    /// @param retryAfter the RetryInfo-requested backoff before the next attempt
    record RetryInfo(Duration retryAfter) implements ProgrammedResponse {}

    /// A non-retryable rejection.
    record Permanent() implements ProgrammedResponse {}

    /// Holds the attempt open for `duration` before answering, so a per-attempt deadline shorter than
    /// `duration` fires and abandons the attempt.
    ///
    /// @param duration how long to hold the attempt open before answering
    record Hang(Duration duration) implements ProgrammedResponse {}
}
