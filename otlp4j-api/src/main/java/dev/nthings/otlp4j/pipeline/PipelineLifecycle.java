package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// Collects and drains lifecycle resources registered along a pipeline graph.
final class PipelineLifecycle {

    private PipelineLifecycle() {
    }

    static List<AutoCloseable> leafResources(Sink<?> terminal) {
        var collected = new ArrayList<AutoCloseable>();
        collectLifecycle(terminal, collected);
        return collected;
    }

    /// Auto-collects lifecycle from the terminal: [FanOut] peers and any node that implements
    /// [AutoCloseable], including exporter signal facets (which carry the exporter's lifecycle).
    /// A resource the pipeline cannot see - hidden behind a lambda sink or a connector's downstream -
    /// is reached only when registered with [Pipeline.Stage#owns(AutoCloseable)].
    private static void collectLifecycle(Object node, List<AutoCloseable> out) {
        if (node instanceof FanOut<?> f) {
            for (var peer : f.peers()) {
                collectLifecycle(peer, out);
            }
        } else if (node instanceof AutoCloseable c) {
            out.add(c);
        }
    }

    static List<AutoCloseable> combine(List<AutoCloseable> a, List<AutoCloseable> b) {
        if (a.isEmpty())
            return b;
        if (b.isEmpty())
            return a;
        var combined = new ArrayList<AutoCloseable>(a.size() + b.size());
        combined.addAll(a);
        combined.addAll(b);
        return combined;
    }

    static PipelineHandle subscription(PipelineHandle sourceSubscription, List<AutoCloseable> resources) {
        return new PipelineSubscription(sourceSubscription, resources);
    }

    static final class PipelineSubscription implements PipelineHandle {

        private final PipelineHandle sourceSubscription;
        private final List<AutoCloseable> resources;

        PipelineSubscription(PipelineHandle sourceSubscription, List<AutoCloseable> resources) {
            this.sourceSubscription = sourceSubscription;
            this.resources = Collections.unmodifiableList(resources);
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            var deadlineNanos = deadlineNanos(timeout);
            var firstError = new AtomicReference<@Nullable Throwable>();
            // Best-effort teardown: stop the source, then close every owned resource even if
            // an earlier step failed, all within a single deadline.
            var chain = settle(sourceSubscription.shutdown(remaining(deadlineNanos)), firstError);
            for (var resource : resources) {
                chain = chain.thenCompose(
                        ignored -> settle(closeResource(resource, remaining(deadlineNanos)), firstError));
            }
            return chain.thenCompose(ignored -> {
                var error = firstError.get();
                return error == null
                        ? CompletableFuture.completedFuture(null)
                        : CompletableFuture.<Void>failedFuture(error);
            });
        }

        @Override
        public CompletionStage<Void> forceFlush(Duration timeout) {
            var deadlineNanos = deadlineNanos(timeout);
            // Flush the source first (a buffering source would otherwise be skipped), then
            // each Lifecycle resource in turn, all sharing one deadline.
            var chained = sourceSubscription.forceFlush(remaining(deadlineNanos)).toCompletableFuture();
            for (var resource : resources) {
                if (resource instanceof Lifecycle f) {
                    chained = chained.thenCompose(v -> f.forceFlush(remaining(deadlineNanos)).toCompletableFuture());
                }
            }
            return chained;
        }

        private static long deadlineNanos(Duration timeout) {
            try {
                return Math.addExact(System.nanoTime(), timeout.toNanos());
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        private static Duration remaining(long deadlineNanos) {
            try {
                return Duration.ofNanos(Math.max(0L, Math.subtractExact(deadlineNanos, System.nanoTime())));
            } catch (ArithmeticException e) {
                return Duration.ofNanos(Long.MAX_VALUE);
            }
        }

        /// Runs a stage to completion, recording the first failure so teardown continues.
        private static CompletableFuture<Void> settle(
                CompletionStage<Void> stage, AtomicReference<@Nullable Throwable> firstError) {
            return stage.toCompletableFuture().handle((ignored, error) -> {
                if (error != null) {
                    firstError.compareAndSet(null, unwrap(error));
                }
                return (Void) null;
            });
        }

        private static Throwable unwrap(Throwable error) {
            return error instanceof CompletionException completion && completion.getCause() != null
                    ? completion.getCause()
                    : error;
        }

        private static CompletionStage<Void> closeResource(AutoCloseable resource, Duration timeout) {
            if (resource instanceof Lifecycle d) {
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
