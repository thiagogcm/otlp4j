package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;

/// Fluent builder for a per-signal consumer chain.
///
/// `Pipeline.from(source)` opens a [Stage] that transforms or filters the batch and
/// is terminated by `.to(consumer)`. To fan out to several peers, terminate with a
/// [FanOut]: `.to(FanOut.of(a, b))`. The returned [PipelineHandle] owns the wiring:
/// closing it detaches every leaf and releases any lifecycle resources attached
/// along the way.
public final class Pipeline {

    private Pipeline() {
    }

    /// Opens a builder attached to `source`.
    ///
    /// @param source the signal source
    /// @param <T>    the signal type
    /// @return the initial stage
    public static <T> Stage<T> from(Source<T> source) {
        return new StageImpl<>(Objects.requireNonNull(source, "source"), batch -> batch, null, new ArrayList<>());
    }

    /// A pipeline stage parameterised by the signal currently flowing through it.
    public sealed interface Stage<T> permits StageImpl {

        /// Adds a pure 1-to-1 transform. The transform must return non-null - a null return is a
        /// contract violation surfaced as a permanent rejection. To drop a batch, use
        /// [#filter(Predicate)], typically after a pruning transform leaves the batch empty.
        ///
        /// @param fn the transform function
        /// @return this stage
        Stage<T> transform(Transform<T> fn);

        /// Adds an asynchronous 1-to-1 stage. The pipeline delivers downstream only once the
        /// processor's stage completes, so the transport acknowledgement covers it. The processor
        /// must complete with a non-null batch (see [Processor]); to drop a batch, follow with
        /// [#filter(Predicate)]. A processor that implements [AutoCloseable] is drained
        /// automatically on shutdown, before the terminal's resources.
        ///
        /// @param stage the asynchronous stage
        /// @return this stage
        Stage<T> process(Processor<T> stage);

        /// Drops the batch entirely if `keep` rejects it.
        ///
        /// @param keep the filter predicate
        /// @return this stage
        Stage<T> filter(Predicate<? super T> keep);

        /// Registers a lifecycle resource that the subscription drains on shutdown and flushes on
        /// forceFlush. An escape hatch, needed only for a resource the pipeline cannot see - hidden
        /// behind a lambda sink or a connector's downstream. Exporter facets and other [AutoCloseable]
        /// terminals or fan-out peers are drained automatically.
        ///
        /// @param resource the lifecycle resource
        /// @return this stage
        Stage<T> owns(AutoCloseable resource);

        /// Terminates the pipeline by delivering to `terminal`. An [AutoCloseable] terminal (such as
        /// an exporter facet or a batching processor) is drained automatically on shutdown; use
        /// [#owns(AutoCloseable)] only for a resource the pipeline cannot see. To fan out, pass a
        /// [FanOut]: `.to(FanOut.of(a, b))`.
        ///
        /// @param terminal the terminal sink
        /// @return the subscription handle
        PipelineHandle to(Sink<? super T> terminal);
    }

    private static final class StageImpl<T> implements Stage<T> {

        final Source<T> source;
        /// Synchronous ops (transform/filter) accumulated since the last async boundary;
        /// identity at the start. Null in, null out: a dropped batch stays dropped.
        final Function<@Nullable T, @Nullable T> pendingSync;
        /// The chain of stages up to the last [Processor], or null while the pipeline is fully
        /// synchronous (the common case pays no future allocation per batch).
        final @Nullable Function<T, CompletableFuture<@Nullable T>> asyncChain;
        final List<AutoCloseable> resources;

        StageImpl(
                Source<T> source,
                Function<@Nullable T, @Nullable T> pendingSync,
                @Nullable Function<T, CompletableFuture<@Nullable T>> asyncChain,
                List<AutoCloseable> resources) {
            this.source = source;
            this.pendingSync = pendingSync;
            this.asyncChain = asyncChain;
            this.resources = resources;
        }

        @Override
        public Stage<T> transform(Transform<T> fn) {
            Objects.requireNonNull(fn, "fn");
            Function<@Nullable T, @Nullable T> step = batch -> batch == null
                    ? null
                    : Objects.requireNonNull(fn.apply(batch),
                            () -> "transform returned null (transforms are 1-to-1; drop batches with filter(...))");
            return new StageImpl<>(source, pendingSync.andThen(step), asyncChain, resources);
        }

        @Override
        public Stage<T> process(Processor<T> stage) {
            Objects.requireNonNull(stage, "stage");
            var lifted = liftedChain();
            Function<T, CompletableFuture<@Nullable T>> next = batch ->
                    lifted.apply(batch).thenCompose(after -> after == null
                            ? CompletableFuture.completedFuture(null)
                            : invokeProcessor(stage, after));
            var nextResources = resources;
            if (stage instanceof AutoCloseable closeable) {
                nextResources = new ArrayList<>(resources.size() + 1);
                nextResources.addAll(resources);
                nextResources.add(closeable);
            }
            return new StageImpl<>(source, batch -> batch, next, nextResources);
        }

        @Override
        public Stage<T> filter(Predicate<? super T> keep) {
            Function<@Nullable T, @Nullable T> filtered = batch -> batch != null && keep.test(batch) ? batch : null;
            return new StageImpl<>(source, pendingSync.andThen(filtered), asyncChain, resources);
        }

        @Override
        public Stage<T> owns(AutoCloseable resource) {
            Objects.requireNonNull(resource, "resource");
            var next = new ArrayList<AutoCloseable>(resources.size() + 1);
            next.addAll(resources);
            next.add(resource);
            return new StageImpl<>(source, pendingSync, asyncChain, next);
        }

        @Override
        public PipelineHandle to(Sink<? super T> terminal) {
            Objects.requireNonNull(terminal, "terminal");
            var chain = asyncChain == null ? syncChainSink(terminal) : asyncChainSink(terminal);
            var leafResources = PipelineLifecycle.leafResources(terminal);
            return PipelineLifecycle.subscription(
                    source.subscribe(chain), PipelineLifecycle.combine(resources, leafResources));
        }

        /// The fully-synchronous fast path: no future allocation until the terminal.
        private Sink<T> syncChainSink(Sink<? super T> terminal) {
            var stageFn = pendingSync;
            return batch -> {
                @Nullable
                T after;
                try {
                    after = stageFn.apply(batch);
                } catch (RuntimeException e) {
                    return CompletableFuture.completedFuture(
                            ConsumeResult.permanent("pipeline stage threw: " + e.getMessage(), e));
                }
                if (after == null) {
                    return ConsumeResult.acceptedStage();
                }
                return deliver(terminal, after);
            };
        }

        /// The asynchronous path: runs the lifted chain, then normalizes drop/failure and
        /// delivers to the terminal.
        private Sink<T> asyncChainSink(Sink<? super T> terminal) {
            var lifted = liftedChain();
            return batch -> lifted.apply(batch)
                    .<CompletionStage<ConsumeResult>>handle((after, failure) -> {
                        if (failure != null) {
                            return CompletableFuture.completedFuture(
                                    rejectedStage("pipeline stage failed", failure));
                        }
                        if (after == null) {
                            return ConsumeResult.acceptedStage();
                        }
                        return deliver(terminal, after);
                    })
                    .thenCompose(stage -> stage);
        }

        /// Lifts the accumulated stages into one future-returning function: the async chain so
        /// far (when present), then the pending synchronous ops. Synchronous throws become
        /// exceptional completions.
        private Function<T, CompletableFuture<@Nullable T>> liftedChain() {
            var sync = pendingSync;
            var chain = asyncChain;
            if (chain == null) {
                return batch -> {
                    try {
                        return CompletableFuture.completedFuture(sync.apply(batch));
                    } catch (RuntimeException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                };
            }
            return batch -> chain.apply(batch).thenApply(sync);
        }
    }

    /// Invokes a processor, normalizing its failure modes into the returned future: a
    /// synchronous throw, a null stage, or a null completion value each complete exceptionally.
    private static <T> CompletableFuture<@Nullable T> invokeProcessor(Processor<T> stage, T batch) {
        CompletionStage<T> result;
        try {
            result = Objects.requireNonNull(stage.process(batch), "processor returned a null stage");
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
        return result.thenApply(value -> Objects.<@Nullable T>requireNonNull(value,
                        "processor completed null (processors are 1-to-1; drop batches with filter(...))"))
                .toCompletableFuture();
    }

    private static <T> CompletionStage<ConsumeResult> deliver(Sink<? super T> terminal, T batch) {
        CompletionStage<ConsumeResult> stage;
        try {
            stage = Objects.requireNonNull(terminal.consume(batch), "terminal returned a null stage");
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(rejectedStage("pipeline terminal threw", e));
        }
        return stage.exceptionally(t -> rejectedStage("pipeline terminal failed", t));
    }

    /// Permanent rejection for a failed pipeline stage or terminal, unwrapping
    /// [CompletionException]. An [Error] is rethrown; an [InterruptedException] restores the
    /// interrupt flag.
    private static ConsumeResult rejectedStage(String prefix, Throwable failure) {
        var cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return ConsumeResult.permanent(prefix + ": " + cause, cause);
    }
}
