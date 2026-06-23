package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.pipeline.Subscription;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/// [Source] backed by a single-consumer slot.
///
/// Throws on a second [#subscribe] attach — multi-consumer fan-out is the caller's
/// responsibility via `FanOut` in the pipeline package.
final class SignalSource<T> implements Source<T> {

    private final Class<T> signalType;
    private final AtomicReference<Consumer<? super T>> attached = new AtomicReference<>();

    public SignalSource(Class<T> signalType) {
        this.signalType = Objects.requireNonNull(signalType, "signalType");
    }

    @Override
    public Subscription subscribe(Consumer<? super T> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (!attached.compareAndSet(null, consumer)) {
            throw new IllegalStateException(
                    "source for " + signalType.getSimpleName() + " already has a consumer; wrap with FanOut for multi-consumer attach");
        }
        return new Subscription() {
            @Override
            public CompletionStage<Void> shutdown(Duration timeout) {
                attached.compareAndSet(consumer, null);
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    /// Sync and async failures propagate so the transport returns gRPC INTERNAL — throwing is
    /// the documented way to surface a transport-level failure.
    public CompletionStage<ConsumeResult<T>> dispatch(T batch) {
        var c = attached.get();
        if (c == null) {
            return ConsumeResult.acceptedStage();
        }
        @SuppressWarnings("unchecked")
        var typed = (Consumer<T>) c;
        return typed.consume(batch);
    }
}
