package dev.nthings.otlp4j.transport.http.internal;

import java.time.Duration;
import org.jspecify.annotations.Nullable;

/// Signals a non-success OTLP/HTTP export after all retries were exhausted. Surfaced as the failed
/// stage's cause, mirroring how the gRPC client surfaces a non-OK status.
final class OtlpHttpException extends RuntimeException {

    private final int statusCode;
    private final @Nullable Duration retryAfter;

    OtlpHttpException(String message, int statusCode, @Nullable Duration retryAfter) {
        super(message);
        this.statusCode = statusCode;
        this.retryAfter = retryAfter;
    }

    int statusCode() {
        return statusCode;
    }

    @Nullable Duration retryAfter() {
        return retryAfter;
    }
}
