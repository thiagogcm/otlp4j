package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import java.time.Clock;
import java.time.Duration;

/// The per-invocation [ConsumeContext] the receiver adapter synthesizes for each decoded batch it
/// routes into the graph. The legacy transport dispatch carries no request-scoped context, so this
/// context reports empty client metadata, a deadline a fixed budget out from the component clock, and
/// no cancellation. Capturing the real per-request principal, headers, and deadline from the wire is
/// a later-phase concern; the context is introduced now so every consumer already has one and no
/// signature widens when the transport begins populating it.
final class ReceiverConsumeContext implements ConsumeContext {

    /// The placeholder per-request budget until the transport captures a real request deadline.
    static final Duration DEFAULT_BUDGET = Duration.ofSeconds(30);

    private final Deadline deadline;

    ReceiverConsumeContext(Clock clock) {
        this.deadline = Deadline.after(DEFAULT_BUDGET, clock);
    }

    @Override
    public ClientMetadata metadata() {
        return ClientMetadata.empty();
    }

    @Override
    public Deadline deadline() {
        return deadline;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public void onCancel(Runnable callback) {
        // The legacy transport surfaces no cancellation signal, so the callback never fires.
    }
}
