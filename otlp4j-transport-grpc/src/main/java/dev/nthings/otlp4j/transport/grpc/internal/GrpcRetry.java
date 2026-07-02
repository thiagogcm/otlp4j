package dev.nthings.otlp4j.transport.grpc.internal;

import io.grpc.Metadata;

/// gRPC retry-pushback wire constants shared by the client (reads the trailer to fold server
/// backpressure into its retry intervals) and the server (emits it for a throttled rejection).
final class GrpcRetry {

    /// Trailer carrying the server's requested retry delay in milliseconds; a negative value
    /// tells the client not to retry at all.
    static final Metadata.Key<String> PUSHBACK_MS =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);

    private GrpcRetry() {
    }
}
