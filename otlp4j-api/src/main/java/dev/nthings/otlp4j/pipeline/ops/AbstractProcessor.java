package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// Base for a stateful, same-signal [Processor] that carries lifecycle. A subclass implements
/// [#process] and, if it buffers or holds resources, overrides [#onShutdown] / [#onForceFlush] to
/// drain local state - reaching the chain continuation through [#next] when the drain must forward
/// downstream. The base owns naming, the captured-`next` accessor, an idempotent shutdown, and the
/// lifecycle surface.
///
/// This base is **single-attachment**: it implements [Lifecycle], not [SharedLifecycle], so one
/// instance belongs to one pipeline chain. A stateful stage genuinely shared across pipelines must
/// implement [SharedLifecycle] itself with a real reference count (as `ClientExporter.Facet` and
/// [Connector] do); a plain [Lifecycle] shared across two subscriptions is shut down once per
/// subscription. Stateless reuse is unrestricted - use [Processors].
///
/// Unlike [Connector], the base does **not** cascade shutdown/flush to the downstream: mid-chain,
/// `next` is the pipeline's own continuation, which the pipeline drains structurally, so cascading
/// here would double-drain it. A captured-downstream cascade comes from [Processor#asSink(Sink)].
///
/// @param <T> the signal processed
public abstract class AbstractProcessor<T> implements Processor<T>, Lifecycle {

    private final String name;
    private final AtomicReference<@Nullable Sink<? super T>> next = new AtomicReference<>();
    private final AtomicBoolean shutDown = new AtomicBoolean();

    /// @param name the processor name, used in log and diagnostic messages
    protected AbstractProcessor(String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    /// The processor name.
    ///
    /// @return the processor name
    protected final String name() { return name; }

    /// The chain continuation captured by [#bindNext], or null if [#process] has not run yet. Stable
    /// within one pipeline; a buffering subclass forwards drained state here from [#onShutdown].
    ///
    /// @return the captured continuation, or null before the first [#process]
    protected final @Nullable Sink<? super T> next() { return next.get(); }

    /// Captures the continuation on the first [#process] call so [#next] can reach it on drain. A
    /// buffering subclass calls this at the top of `process`. Attaching one instance to two chains
    /// (two distinct continuations) fails fast - single-attachment (see class doc).
    ///
    /// @param forward the chain continuation handed to `process`
    protected final void bindNext(Sink<? super T> forward) {
        var prev = next.compareAndExchange(null, forward);
        if (prev != null && prev != forward) {
            throw new IllegalStateException(name + ": processor bound to two downstreams");
        }
    }

    /// Processes one batch and forwards to `next`. A buffering subclass should call
    /// [#bindNext(Sink)] first. A throw, a null stage, or a failed stage is normalized by the
    /// attachment boundary into a permanent rejection (see [dev.nthings.otlp4j.pipeline.Pipeline]).
    @Override public abstract CompletionStage<ConsumeResult> process(T batch, Sink<? super T> next);

    @Override public final CompletionStage<Void> shutdown(Duration timeout) {
        if (!shutDown.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);   // idempotent: reached twice, drained once
        }
        return onShutdown(timeout);
    }

    @Override public final CompletionStage<Void> forceFlush(Duration timeout) { return onForceFlush(timeout); }

    /// Drains processor-local state on shutdown (default no-op). Runs while the continuation is still
    /// open, because the pipeline drains this processor before the downstream (registration order),
    /// so a buffering subclass may flush to [#next].
    ///
    /// @param timeout the drain deadline
    /// @return a stage completing when the local state is drained
    protected CompletionStage<Void> onShutdown(Duration timeout)   { return CompletableFuture.completedFuture(null); }

    /// Flushes processor-local state on forceFlush (default no-op); may flush to [#next].
    ///
    /// @param timeout the flush deadline
    /// @return a stage completing when the local state is flushed
    protected CompletionStage<Void> onForceFlush(Duration timeout) { return CompletableFuture.completedFuture(null); }

    @Override public String toString() { return name; }
}
