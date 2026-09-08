package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import java.util.concurrent.CompletionStage;

/// Wraps one component consumer so every delivery through it is normalized by
/// [GraphNodeBoundary]: the wrapped consumer's own throws, null stages, and failed stages all become
/// delivery results carrying the component's provenance. Every graph edge delivers through a guarded
/// consumer, so exception normalization happens in exactly one place regardless of which component is
/// on the receiving end. Capabilities pass through unchanged so a fan-out upstream still reads the
/// wrapped consumer's mutation declaration.
///
/// @param <T> the signal domain type
public final class GuardedConsumer<T> implements SignalConsumer<T> {

    private final SignalConsumer<T> delegate;

    private final Provenance provenance;

    /// Wraps `delegate`, stamping `provenance` on any normalized failure.
    ///
    /// @param delegate   the component consumer to wrap
    /// @param provenance the provenance to stamp on a normalized failure
    public GuardedConsumer(SignalConsumer<T> delegate, Provenance provenance) {
        this.delegate = delegate;
        this.provenance = provenance;
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        return GraphNodeBoundary.deliver(delegate, provenance, context, data);
    }
}
