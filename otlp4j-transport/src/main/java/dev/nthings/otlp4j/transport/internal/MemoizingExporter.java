package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.codec.PayloadWire;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Gives a payload its memoized wire form before it enters an exporter queue. A payload built from a
/// domain batch is re-encoded by every export that sees it, and a queue exports the same payload again
/// for each redelivery. Memoizing here, outside the queue, means the queue holds one payload that
/// encodes once, on its first attempt, and every retry reuses the bytes. A payload that arrived over
/// OTLP already carries its request and passes through unchanged.
///
/// @param <T> the signal's domain batch type
public final class MemoizingExporter<T> implements Exporter<T> {

    private final Exporter<T> delegate;
    private final Provenance provenance;

    /// Wraps `delegate`, which is the queue in front of the wire exporter.
    ///
    /// @param delegate   the exporter to hand memoized payloads to
    /// @param id         the exporter's component id, for results this wrapper produces
    /// @param signal     the signal this exporter carries
    public MemoizingExporter(Exporter<T> delegate, ComponentId id, SignalDescriptor<T> signal) {
        this.delegate = delegate;
        this.provenance = new Provenance(id, ComponentKind.EXPORTER, signal);
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        Payload<T> memoized;
        try {
            memoized = PayloadWire.memoize(data);
        } catch (RuntimeException e) {
            // The payload cannot be marshalled; re-sending the same payload cannot change that.
            return CompletableFuture.completedFuture(
                    DeliveryResult.permanentFrom("failed to prepare the OTLP request", e, provenance));
        }
        return delegate.consume(context, memoized);
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        return delegate.start(context);
    }

    @Override
    public CompletionStage<FlushReport> flush(FlushContext context) {
        return delegate.flush(context);
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        return delegate.shutdown(context);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
