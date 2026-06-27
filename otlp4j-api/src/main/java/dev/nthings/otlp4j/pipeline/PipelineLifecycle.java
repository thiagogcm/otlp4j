package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.Subscription;
import dev.nthings.otlp4j.pipeline.FanOut;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Collects and drains lifecycle resources registered along a pipeline graph.
final class PipelineLifecycle {

    private PipelineLifecycle() {
    }

    static List<AutoCloseable> leafResources(Sink<?> terminal) {
        var collected = new ArrayList<AutoCloseable>();
        collectLifecycle(terminal, collected);
        return collected;
    }

    /// Auto-collects lifecycle from the terminal: [FanOut] peers and any node that
    /// implements [AutoCloseable]. Exporter signal facets are plain sinks and are
    /// not collected; register the exporter explicitly with
    /// [Pipeline.Stage#owns(AutoCloseable)] or the two-arg
    /// [Pipeline.Stage#to(Sink, AutoCloseable)].
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

    static Subscription subscription(Subscription sourceSubscription, List<AutoCloseable> resources) {
        return new PipelineSubscription(sourceSubscription, resources);
    }

    static final class PipelineSubscription implements Subscription {

        private final Subscription sourceSubscription;
        private final List<AutoCloseable> resources;

        PipelineSubscription(Subscription sourceSubscription, List<AutoCloseable> resources) {
            this.sourceSubscription = sourceSubscription;
            this.resources = Collections.unmodifiableList(resources);
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            var deadlineNanos = deadlineNanos(timeout);
            var future = sourceSubscription.shutdown(timeout).toCompletableFuture();
            for (var resource : resources) {
                future = future.thenCompose(v -> closeResource(resource, remaining(deadlineNanos)));
            }
            return future;
        }

        @Override
        public CompletionStage<Void> forceFlush(Duration timeout) {
            var deadlineNanos = deadlineNanos(timeout);
            var chained = CompletableFuture.<Void>completedFuture(null);
            for (var resource : resources) {
                if (resource instanceof Flushable f) {
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

        private static CompletionStage<Void> closeResource(AutoCloseable resource, Duration timeout) {
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
