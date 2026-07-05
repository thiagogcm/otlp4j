package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A [Sink] that forwards each batch through a [Processor] to a captured downstream, cascading
/// lifecycle to the processor (if a [Lifecycle]) then the downstream (if a [Lifecycle]), and
/// reference-counting a [SharedLifecycle] on either side. Behaviourally the same cascade as
/// [dev.nthings.otlp4j.pipeline.ops.Connector]. Failure normalization is shared with the mid-chain
/// path via [Processing#run].
final class BoundProcessorSink<T> implements Sink<T>, SharedLifecycle {

    private final Processor<T> processor;
    private final Sink<? super T> downstream;

    BoundProcessorSink(Processor<T> processor, Sink<? super T> downstream) {
        this.processor = processor;
        this.downstream = downstream;
    }

    @Override public CompletionStage<ConsumeResult> consume(T batch) {
        return Processing.run(processor, batch, downstream);   // one shared normalizer (§2.4)
    }

    @Override public void retain() {
        // A shared processor OR a shared downstream reached only through us must stay reference-counted.
        if (processor instanceof SharedLifecycle sp) sp.retain();
        if (downstream instanceof SharedLifecycle sd) sd.retain();
    }

    @Override public void release() {
        // Mirror retain(): drop one owner's stake on each shared side without draining.
        if (processor instanceof SharedLifecycle sp) sp.release();
        if (downstream instanceof SharedLifecycle sd) sd.release();
    }

    @Override public CompletionStage<Void> shutdown(Duration timeout) {
        return drain(processor, timeout).thenCompose(ignored ->
                downstream instanceof Lifecycle d ? d.shutdown(timeout) : CompletableFuture.completedFuture(null));
    }

    @Override public CompletionStage<Void> forceFlush(Duration timeout) {
        return flush(processor, timeout).thenCompose(ignored ->
                downstream instanceof Lifecycle d ? d.forceFlush(timeout) : CompletableFuture.completedFuture(null));
    }

    private static CompletionStage<Void> drain(Object o, Duration t) {
        return o instanceof Lifecycle d ? d.shutdown(t) : CompletableFuture.completedFuture(null);
    }
    private static CompletionStage<Void> flush(Object o, Duration t) {
        return o instanceof Lifecycle d ? d.forceFlush(t) : CompletableFuture.completedFuture(null);
    }
}
