package dev.nthings.otlp4j.core.signal;

/// A received payload's content is invalid, so [Payload#data()] cannot build its domain batch. The
/// sender is at fault and repeating the request cannot help, which is what sets this apart from a
/// failure of the component that asked for the batch.
public final class UndecodablePayloadException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /// Creates the exception.
    ///
    /// @param message what could not be decoded
    /// @param cause   the failure that decoding raised
    public UndecodablePayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
