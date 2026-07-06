package dev.nthings.otlp4j.testing.conformance;

import io.grpc.Status;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/// What a wire client captured from one exchange, over either transport.
///
/// An HTTP exchange fills the HTTP status, the optional `Retry-After` header, and the response
/// body. A gRPC exchange fills the status code, the status description, the optional
/// `grpc-retry-pushback-ms` trailer, and the response body. The conformance assertions read only
/// through the accessors, never the transport internals.
public final class WireResponse {

    private final int httpStatus;
    private final Status.Code grpcCode;
    private final String grpcDescription;
    private final Long pushbackMillis;
    private final String retryAfterHeader;
    private final byte[] body;

    private WireResponse(
            int httpStatus,
            Status.Code grpcCode,
            String grpcDescription,
            Long pushbackMillis,
            String retryAfterHeader,
            byte[] body) {
        this.httpStatus = httpStatus;
        this.grpcCode = grpcCode;
        this.grpcDescription = grpcDescription;
        this.pushbackMillis = pushbackMillis;
        this.retryAfterHeader = retryAfterHeader;
        this.body = body;
    }

    /// Captures an OTLP/HTTP exchange.
    ///
    /// @param status the HTTP status code
    /// @param retryAfterHeader the `Retry-After` header value, or null when absent
    /// @param body the response body bytes, or null when none
    /// @return the captured response
    public static WireResponse http(int status, String retryAfterHeader, byte[] body) {
        return new WireResponse(status, null, null, null, retryAfterHeader, body);
    }

    /// Captures an OTLP/gRPC exchange.
    ///
    /// @param code the gRPC status code
    /// @param description the gRPC status description, or null when none
    /// @param pushbackMillis the `grpc-retry-pushback-ms` trailer as millis, or null when absent
    /// @param body the response body bytes, or null when the call failed
    /// @return the captured response
    public static WireResponse grpc(Status.Code code, String description, Long pushbackMillis, byte[] body) {
        return new WireResponse(-1, code, description, pushbackMillis, null, body);
    }

    /// The HTTP status code, present only for an HTTP exchange.
    ///
    /// @return the HTTP status, or empty for a gRPC exchange
    public OptionalInt httpStatus() {
        return httpStatus < 0 ? OptionalInt.empty() : OptionalInt.of(httpStatus);
    }

    /// The gRPC status code, present only for a gRPC exchange.
    ///
    /// @return the gRPC status code, or empty for an HTTP exchange
    public Optional<Status.Code> grpcCode() {
        return Optional.ofNullable(grpcCode);
    }

    /// The gRPC status description, used to distinguish an admission refusal from a plain
    /// retryable rejection, which share the `UNAVAILABLE` code.
    ///
    /// @return the status description, or empty when none
    public Optional<String> grpcDescription() {
        return Optional.ofNullable(grpcDescription);
    }

    /// The `grpc-retry-pushback-ms` trailer as milliseconds, present when the server advertised a
    /// backoff hint.
    ///
    /// @return the pushback in millis, or empty when the trailer was absent
    public OptionalLong pushbackMillis() {
        return pushbackMillis == null ? OptionalLong.empty() : OptionalLong.of(pushbackMillis);
    }

    /// The HTTP `Retry-After` header value verbatim, present when the server advertised one.
    ///
    /// @return the header value, or empty when absent
    public Optional<String> retryAfterHeader() {
        return Optional.ofNullable(retryAfterHeader);
    }

    /// The captured response body bytes, which the caller parses with the generated response
    /// message when the exchange succeeded.
    ///
    /// @return the body bytes, or empty when none
    public Optional<byte[]> body() {
        return Optional.ofNullable(body);
    }
}
