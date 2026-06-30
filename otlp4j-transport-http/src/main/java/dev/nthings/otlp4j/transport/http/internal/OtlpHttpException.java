package dev.nthings.otlp4j.transport.http.internal;

/// Signals a non-success OTLP/HTTP export after all retries were exhausted. Surfaced as the failed
/// stage's cause, mirroring how the gRPC client surfaces a non-OK status.
final class OtlpHttpException extends RuntimeException {

    OtlpHttpException(String message) {
        super(message);
    }
}
