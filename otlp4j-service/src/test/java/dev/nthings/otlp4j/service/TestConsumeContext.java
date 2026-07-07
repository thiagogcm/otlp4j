package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import java.time.Clock;
import java.time.Duration;

/// A minimal consume context for the data-flow tests: empty client metadata, a deadline a few seconds
/// out, never cancelled.
final class TestConsumeContext implements ConsumeContext {

    private final Deadline deadline = Deadline.after(Duration.ofSeconds(5), Clock.systemUTC());

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
        // never cancelled in tests
    }
}
