package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.Source;
import dev.nthings.otlp4j.core.Subscription;
import dev.nthings.otlp4j.model.ConsumeResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Fluent builder for a per-signal consumer chain.
///
/// `Pipeline.from(source)` opens a [Stage] that transforms or filters the batch, optionally
/// fans it out, and is terminated by `.to(consumer)` or `.join()` (when a branch is already in
/// play). The returned [Subscription] owns the wiring: closing it detaches every leaf and
/// releases any lifecycle resources attached along the way.
public final class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private Pipeline() {}

    /// Opens a builder attached to `source`.
    public static <T> Stage<T> from(Source<T> source) {
        return new StageImpl<>(Objects.requireNonNull(source, "source"), Function.identity(), new ArrayList<>());
    }

    /// A pipeline stage parameterised by the signal currently flowing through it.
    public sealed interface Stage<T> permits StageImpl {

        /// Adds a pure 1→1 transform.
        Stage<T> transform(Transform<T> fn);

        /// Drops the batch entirely if `keep` rejects it.
        Stage<T> filter(Predicate<? super T> keep);

        /// Adds a fire-and-forget side-effect observer that cannot alter or reject the batch.
        /// Anything `observer` throws (exceptions and `Error`s alike) is caught and logged so it
        /// never affects the main path; for a demand-aware live stream use the receiver's
        /// `TelemetryTap`.
        Stage<T> peek(java.util.function.Consumer<? super T> observer);

        /// Registers a lifecycle resource (e.g. an exporter) that the subscription drains on
        /// shutdown and flushes on forceFlush if it is [Flushable]. Use this to attach resources
        /// reachable only behind method-reference consumers (e.g. `exporter.traces()`).
        Stage<T> owns(AutoCloseable resource);

        /// Opens a branch — subsequent `.fanOut(...)` calls add peers, `.join()` closes the
        /// branch and returns the active subscription.
        Branch<T> branch();

        /// Terminates the pipeline by delivering to `terminal`. Returns the subscription that
        /// owns the wiring.
        Subscription to(Sink<T> terminal);

        /// Terminates the pipeline by delivering to `terminal`, also registering `owner` as a
        /// lifecycle resource so the subscription drains it on shutdown and flushes it on
        /// forceFlush if it is [Flushable]. Shorthand for `owns(owner).to(terminal)`; use it when
        /// the terminal is a method-reference facet such as `exporter.traces()` whose owner the
        /// pipeline cannot otherwise infer.
        default Subscription to(Sink<T> terminal, AutoCloseable owner) {
            return owns(owner).to(terminal);
        }
    }

    /// A branch builder collecting peers for a fan-out.
    public sealed interface Branch<T> permits BranchImpl {

        /// Adds a peer consumer to the fan-out.
        Branch<T> fanOut(Sink<T> peer);

        /// Closes the branch, attaches the fan-out to the source, and returns the subscription.
        Subscription join();
    }

    static final class StageImpl<T> implements Stage<T> {

        final Source<T> source;
        final Function<T, T> stageFn;
        final List<AutoCloseable> resources;

        StageImpl(Source<T> source, Function<T, T> stageFn, List<AutoCloseable> resources) {
            this.source = source;
            this.stageFn = stageFn;
            this.resources = resources;
        }

        @Override
        public Stage<T> transform(Transform<T> fn) {
            Objects.requireNonNull(fn, "fn");
            // A prior filter may have dropped the batch to null; don't hand null to a user Transform.
            Function<T, T> step = batch -> batch == null ? null : fn.apply(batch);
            return new StageImpl<>(source, stageFn.andThen(step), resources);
        }

        @Override
        public Stage<T> filter(Predicate<? super T> keep) {
            Function<T, T> filtered = batch -> batch != null && keep.test(batch) ? batch : null;
            return new StageImpl<>(source, stageFn.andThen(filtered), resources);
        }

        @Override
        public Stage<T> peek(java.util.function.Consumer<? super T> observer) {
            Objects.requireNonNull(observer, "observer");
            Function<T, T> peek = batch -> {
                if (batch != null) {
                    try {
                        observer.accept(batch);
                    } catch (Throwable t) {
                        // Fire-and-forget: a peek failure (even an Error like AssertionError) must not
                        // reach the delivery path. Log so it isn't lost.
                        log.warn("pipeline peek observer threw; ignoring to protect the delivery path", t);
                    }
                }
                return batch;
            };
            return new StageImpl<>(source, stageFn.andThen(peek), resources);
        }

        @Override
        public Stage<T> owns(AutoCloseable resource) {
            Objects.requireNonNull(resource, "resource");
            // Copy rather than mutate: sibling stage values share this `resources` reference, so an
            // in-place append would leak the registration into branches that never asked for it.
            var next = new ArrayList<AutoCloseable>(resources.size() + 1);
            next.addAll(resources);
            next.add(resource);
            return new StageImpl<>(source, stageFn, next);
        }

        @Override
        public Branch<T> branch() {
            return new BranchImpl<>(this);
        }

        @Override
        public Subscription to(Sink<T> terminal) {
            Objects.requireNonNull(terminal, "terminal");
            Sink<T> chain = batch -> {
                T after;
                try {
                    after = stageFn.apply(batch);
                } catch (RuntimeException e) {
                    return CompletableFuture.completedFuture(
                            ConsumeResult.rejected("pipeline stage threw: " + e.getMessage(), e));
                }
                if (after == null) {
                    return ConsumeResult.acceptedStage();
                }
                return terminal.consume(after);
            };
            var leafResources = leafResources(terminal);
            return new PipelineSubscription(source.subscribe(chain), combine(resources, leafResources));
        }
    }

    static final class BranchImpl<T> implements Branch<T> {

        private final StageImpl<T> stage;
        private final List<Sink<T>> peers = new ArrayList<>();

        BranchImpl(StageImpl<T> stage) {
            this.stage = stage;
        }

        @Override
        public Branch<T> fanOut(Sink<T> peer) {
            peers.add(Objects.requireNonNull(peer, "peer"));
            return this;
        }

        @Override
        public Subscription join() {
            if (peers.isEmpty()) {
                throw new IllegalStateException("branch has no fanOut peers");
            }
            return stage.to(FanOut.of(peers));
        }
    }

    private static List<AutoCloseable> leafResources(Sink<?> terminal) {
        var collected = new ArrayList<AutoCloseable>();
        collectLifecycle(terminal, collected);
        return collected;
    }

    /// Auto-collects lifecycle from the terminal: [FanOut] peers and any node that directly
    /// implements [AutoCloseable]. It deliberately does NOT see through method-reference consumers
    /// (`exporter.traces()` is `client::exportTraces`) or connector downstreams — those hide their
    /// owner behind a lambda, so register them explicitly with [Stage#owns(AutoCloseable)].
    private static void collectLifecycle(Object node, List<AutoCloseable> out) {
        if (node instanceof FanOut<?> f) {
            for (var peer : f.peers()) {
                collectLifecycle(peer, out);
            }
        } else if (node instanceof AutoCloseable c) {
            out.add(c);
        }
    }

    private static List<AutoCloseable> combine(List<AutoCloseable> a, List<AutoCloseable> b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        var combined = new ArrayList<AutoCloseable>(a.size() + b.size());
        combined.addAll(a);
        combined.addAll(b);
        return combined;
    }

    static final class PipelineSubscription implements Subscription {

        private final Subscription sourceSubscription;
        private final List<AutoCloseable> resources;

        PipelineSubscription(Subscription sourceSubscription, List<AutoCloseable> resources) {
            this.sourceSubscription = sourceSubscription;
            this.resources = Collections.unmodifiableList(resources);
        }

        /// Order: detach the source first, then drain each leaf resource in registration order.
        /// All steps share a single deadline derived from `timeout`, so each resource is closed
        /// with only the time remaining — total shutdown is bounded by `timeout`, not N × timeout.
        /// Resources registered via [Stage#owns(AutoCloseable)] participate alongside the
        /// auto-collected terminals.
        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            var deadlineNanos = System.nanoTime() + timeout.toNanos();
            var future = sourceSubscription.shutdown(timeout).toCompletableFuture();
            for (var resource : resources) {
                future = future.thenCompose(v -> {
                    var remaining = Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime()));
                    return closeResource(resource, remaining);
                });
            }
            return future;
        }

        @Override
        public CompletionStage<Void> forceFlush(Duration timeout) {
            var chained = CompletableFuture.<Void>completedFuture(null);
            for (var resource : resources) {
                if (resource instanceof Flushable f) {
                    chained = chained.thenCompose(v -> f.forceFlush(timeout).toCompletableFuture());
                }
            }
            return chained;
        }

        private static CompletionStage<Void> closeResource(AutoCloseable resource, Duration timeout) {
            // A Subscription and the exporter are both Drainable, so they drain within the remaining
            // shared budget rather than via their own fixed-timeout close().
            if (resource instanceof Drainable d) {
                return d.shutdown(timeout);
            }
            try {
                resource.close();
            } catch (Exception e) {
                return CompletableFuture.failedStage(e);
            }
            return CompletableFuture.completedFuture(null);
        }
    }
}
