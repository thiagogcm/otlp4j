package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// A minimal consume context for the component tests, carrying programmable client metadata so the
/// metadata-keyed batching path is exercisable, a deadline a fixed budget out, never cancelled.
public final class TestConsumeContext implements ConsumeContext {

    private final ClientMetadata metadata;
    private final Deadline deadline = Deadline.after(Duration.ofSeconds(30), Clock.systemUTC());

    /// A context with empty client metadata.
    public TestConsumeContext() {
        this(ClientMetadata.empty());
    }

    /// A context carrying the given client metadata.
    ///
    /// @param metadata the client metadata to expose
    public TestConsumeContext(ClientMetadata metadata) {
        this.metadata = metadata;
    }

    /// A context carrying one header key with the given values.
    ///
    /// @param key    the header name
    /// @param values the header values
    /// @return the context
    public static TestConsumeContext withHeader(String key, String... values) {
        return new TestConsumeContext(new ClientMetadata(Optional.empty(), Map.of(key, List.of(values))));
    }

    @Override
    public ClientMetadata metadata() {
        return metadata;
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
