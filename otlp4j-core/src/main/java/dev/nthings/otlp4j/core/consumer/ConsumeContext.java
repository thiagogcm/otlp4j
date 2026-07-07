package dev.nthings.otlp4j.core.consumer;

/// The per-invocation context that flows through every consume call: the request-scoped client
/// metadata, the deadline the work must complete within, and the cancellation signal. This context
/// appears in every consumer signature, so it is introduced up front: the legacy consume signature
/// carried no context at all, which is exactly the gap that made metadata-based routing and
/// deadline-aware delivery inexpressible.
public interface ConsumeContext {

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
