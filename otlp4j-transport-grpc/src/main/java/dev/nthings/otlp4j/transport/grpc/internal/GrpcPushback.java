package dev.nthings.otlp4j.transport.grpc.internal;

import io.grpc.Metadata;
import java.time.Duration;
import org.jspecify.annotations.Nullable;

/// gRPC retry-pushback wire encoding shared by the client (reads the trailer to fold server
/// backpressure into its retry intervals) and the server (emits it for a throttled rejection).
final class GrpcPushback {

    /// Trailer carrying the server's requested retry delay in milliseconds; a negative value
    /// tells the client not to retry at all.
    static final Metadata.Key<String> PUSHBACK_MS =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);

    private GrpcPushback() {
    }

    /// Trailers carrying `hint` as the pushback delay, or null (no trailers) when there is no
    /// hint.
    static @Nullable Metadata pushbackTrailers(@Nullable Duration hint) {
        if (hint == null) {
            return null;
        }
        var trailers = new Metadata();
        trailers.put(PUSHBACK_MS, Long.toString(hint.toMillis()));
        return trailers;
    }
}
