package dev.nthings.otlp4j.core.consumer;

import java.util.Objects;

/// The per-invocation context that flows through every consume call: request-scoped client metadata,
/// the deadline the work must complete within, and a cancellation signal.
public interface ConsumeContext {

    /// A context with empty client metadata and no deadline.
    static ConsumeContext defaults() {
        return new DefaultConsumeContext(ClientMetadata.empty(), Deadline.never());
    }

    /// A context with the supplied immutable metadata and deadline.
    static ConsumeContext of(ClientMetadata metadata, Deadline deadline) {
        return new DefaultConsumeContext(
                Objects.requireNonNull(metadata, "metadata"),
                Objects.requireNonNull(deadline, "deadline"));
    }

    /// The request-scoped client metadata the receiver captured.
    ///
    /// @return the client metadata
    ClientMetadata metadata();

    /// The deadline the consume work must complete within.
    ///
    /// @return the deadline
    Deadline deadline();

    /// Whether the request has been cancelled (the client went away or the deadline elapsed). A
    /// consumer SHOULD check this before doing expensive work.
    ///
    /// @return true once the request is cancelled
    boolean isCancelled();

    /// Registers a callback to run when the request is cancelled. If the request is already
    /// cancelled, the callback runs immediately.
    ///
    /// @param callback the cancellation callback
    void onCancel(Runnable callback);
}
