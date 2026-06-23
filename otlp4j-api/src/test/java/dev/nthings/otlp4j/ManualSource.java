package dev.nthings.otlp4j;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.Source;
import dev.nthings.otlp4j.core.Subscription;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/// Test [Source] with a single-consumer slot and a manual [#dispatch] feed, so pipeline tests can
/// push batches without standing up a transport. Mirrors the production receiver source, which is
/// package-private receiver plumbing.
final class ManualSource<T> implements Source<T> {

    private final AtomicReference<Sink<? super T>> attached = new AtomicReference<>();

    @Override
    public Subscription subscribe(Sink<? super T> consumer) {
        if (!attached.compareAndSet(null, consumer)) {
            throw new IllegalStateException("source already has a consumer");
        }
        return timeout -> {
            attached.compareAndSet(consumer, null);
            return CompletableFuture.completedFuture(null);
        };
    }

    /// Feeds a batch to the attached consumer, or completes with [ConsumeResult#acceptedStage()] when unattached.
    CompletionStage<ConsumeResult<T>> dispatch(T batch) {
        var c = attached.get();
        if (c == null) {
            return ConsumeResult.acceptedStage();
        }
        @SuppressWarnings("unchecked")
        var typed = (Sink<T>) c;
        return typed.consume(batch);
    }
}
