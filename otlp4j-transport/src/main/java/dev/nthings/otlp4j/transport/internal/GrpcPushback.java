package dev.nthings.otlp4j.transport.internal;

import io.grpc.Metadata;
import io.grpc.Status;
import java.time.Duration;
import org.jspecify.annotations.Nullable;

/// gRPC retry-pushback wire encoding shared by the Armeria client (reads the `grpc-retry-pushback-ms`
/// trailer to fold server backpressure into its retry intervals) and the Armeria gRPC service (emits
/// it for a throttled or admission rejection). The standard trailer lets clients and servers
/// interoperate across both OTLP protocols.
///
/// The service also attaches the spec-compliant `grpc-status-details-bin` `RetryInfo` detail encoding
/// the same delay, so a spec-compliant client that reads only `RetryInfo` (and not otlp4j's pushback
/// trailer) still honours the backoff.
final class GrpcPushback {

    /// Trailer carrying the server's requested retry delay in milliseconds; a negative value tells
    /// the client not to retry at all.
    static final Metadata.Key<String> PUSHBACK_MS =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);

    /// Binary trailer carrying a protobuf status whose details may include a `RetryInfo`. The
    /// `-bin` suffix marks it binary; gRPC base64-encodes it on the wire.
    static final Metadata.Key<byte[]> STATUS_DETAILS_BIN =
            Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER);

    /// The numeric gRPC `UNAVAILABLE` status the throttle detail wraps (both admission refusal and a
    /// retryable rejection map to `UNAVAILABLE`).
    private static final int UNAVAILABLE = Status.Code.UNAVAILABLE.value();

    private GrpcPushback() {}

    /// Trailers carrying `hint` both as the `grpc-retry-pushback-ms` pushback delay and as a
    /// spec-compliant `RetryInfo` detail in `grpc-status-details-bin`, or null (no trailers) when there
    /// is no hint.
    ///
    /// @param hint the requested retry delay, or null for none
    /// @return the throttle trailers, or null when there is no hint
    static @Nullable Metadata pushbackTrailers(@Nullable Duration hint) {
        if (hint == null) {
            return null;
        }
        var trailers = new Metadata();
        trailers.put(PUSHBACK_MS, Long.toString(hint.toMillis()));
        trailers.put(STATUS_DETAILS_BIN,
                RetryInfoCodec.encodeRetryInfo(UNAVAILABLE, "retry after " + hint.toMillis() + " ms", hint));
        return trailers;
    }
}
