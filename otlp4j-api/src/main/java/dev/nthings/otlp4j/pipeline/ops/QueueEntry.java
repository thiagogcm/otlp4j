package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/// One queued batch plus, in [AckMode#ON_DELIVERY] mode, the caller's ack to complete on drain
/// (null in [AckMode#ON_ENQUEUE] mode, where the caller was already acked at enqueue). Identity
/// equality on purpose: the shutdown-race reclaim (`queue.remove(entry)`) and DROP_OLDEST eviction
/// must match THIS entry, never a structurally-equal batch from another caller.
final class QueueEntry<T> {

    final T batch;
    final @Nullable CompletableFuture<ConsumeResult> ack;

    QueueEntry(T batch, @Nullable CompletableFuture<ConsumeResult> ack) {
        this.batch = batch;
        this.ack = ack;
    }
    // no equals/hashCode override -> identity (Object) semantics
}
