package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.Source;
import dev.nthings.otlp4j.core.PipelineHandle;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// [Source] backed by a single-consumer slot.
///
/// Throws on second [SignalSource#subscribe]. Multi-consumer fan-out is the
/// caller's responsibility.
final class SignalSource<T> implements Source<T> {

    private final Class<T> signalType;
    private final ConsumeResult<T> noConsumerResult;
    private final AtomicReference<@Nullable Sink<? super T>> attached = new AtomicReference<>();

    public SignalSource(Class<T> signalType) {
        this.signalType = Objects.requireNonNull(signalType, "signalType");
        this.noConsumerResult = ConsumeResult.retryable(
                "no consumer attached for " + signalType.getSimpleName()
                        + "; attach a sink or call discard() to accept and drop");
    }

    @Override
    public PipelineHandle subscribe(Sink<? super T> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (!attached.compareAndSet(null, consumer)) {
            throw new IllegalStateException(
                    "source for " + signalType.getSimpleName() + " already has a consumer; wrap with FanOut for multi-consumer attach");
        }
        return new PipelineHandle() {
            @Override
            public CompletionStage<Void> shutdown(Duration timeout) {
                attached.compareAndSet(consumer, null);
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    /// Forwards the batch verbatim. A direct subscriber's throw or failed stage is NOT
    /// normalized into [ConsumeResult.Rejected] - it propagates so the transport renders
    /// gRPC `INTERNAL` / HTTP `500`. Pipeline/[FanOut]/[Sink] adapters normalize first.
    public CompletionStage<ConsumeResult<T>> dispatch(T batch) {
        @Nullable Sink<? super T> sink = attached.get();
        if (sink == null) {
            return CompletableFuture.completedFuture(noConsumerResult);
        }
        @SuppressWarnings("unchecked")
        var typed = (Sink<T>) sink;
        return typed.consume(batch);
    }
}
