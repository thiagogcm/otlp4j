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
        return new StageImpl<>(Objects.requireNonNull(source, "source"), batch -> batch, new ArrayList<>());
    }

    /// A pipeline stage parameterised by the signal currently flowing through it.
    public sealed interface Stage<T> permits StageImpl {

        /// Adds a pure 1-to-1 transform. A transform that returns null rejects the batch.
        ///
        /// @param fn the transform function
        /// @return this stage
        Stage<T> transform(Transform<T> fn);

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
        final Function<T, @Nullable T> stageFn;
        final List<AutoCloseable> resources;

        StageImpl(Source<T> source, Function<T, @Nullable T> stageFn, List<AutoCloseable> resources) {
            this.source = source;
            this.stageFn = stageFn;
            this.resources = resources;
        }

        @Override
        public Stage<T> transform(Transform<T> fn) {
            Objects.requireNonNull(fn, "fn");
            Function<@Nullable T, @Nullable T> step = batch -> batch == null
                    ? null
                    : Objects.requireNonNull(fn.apply(batch), "pipeline transform returned null");
            return new StageImpl<>(source, stageFn.andThen(step), resources);
        }

        @Override
        public Stage<T> filter(Predicate<? super T> keep) {
            Function<@Nullable T, @Nullable T> filtered = batch -> batch != null && keep.test(batch) ? batch : null;
            return new StageImpl<>(source, stageFn.andThen(filtered), resources);
        }

        @Override
        public Stage<T> owns(AutoCloseable resource) {
            Objects.requireNonNull(resource, "resource");
            var next = new ArrayList<AutoCloseable>(resources.size() + 1);
            next.addAll(resources);
            next.add(resource);
            return new StageImpl<>(source, stageFn, next);
        }

        @Override
        public PipelineHandle to(Sink<? super T> terminal) {
            Objects.requireNonNull(terminal, "terminal");
            Sink<T> chain = batch -> {
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
            var leafResources = PipelineLifecycle.leafResources(terminal);
            return PipelineLifecycle.subscription(
                    source.subscribe(chain), PipelineLifecycle.combine(resources, leafResources));
        }
    }

    private static <T> CompletionStage<ConsumeResult<T>> deliver(Sink<? super T> terminal, T batch) {
        CompletionStage<? extends ConsumeResult<?>> stage;
        try {
            stage = Objects.requireNonNull(terminal.consume(batch), "terminal returned a null stage");
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(rejectedTerminal("pipeline terminal threw", e));
        }
        return Pipeline.<T>retag(stage).exceptionally(t -> rejectedTerminal("pipeline terminal failed", t));
    }

    private static <T> ConsumeResult<T> rejectedTerminal(String prefix, Throwable failure) {
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

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<ConsumeResult<T>> retag(CompletionStage<? extends ConsumeResult<?>> stage) {
        return (CompletionStage<ConsumeResult<T>>) stage;
    }
}
