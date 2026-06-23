package dev.nthings.otlp4j.internal;

/// Signals a non-success OTLP/HTTP export: the server returned a status outside 2xx (after any
/// retries were exhausted). Surfaced to the caller as the failed stage's cause, mirroring how the
/// gRPC client surfaces a non-OK status. The status code is included in the message.
final class OtlpHttpException extends RuntimeException {

    OtlpHttpException(String message) {
        super(message);
    }
}
