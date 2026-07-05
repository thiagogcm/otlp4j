package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// Collects and drains lifecycle resources registered along a pipeline graph. The
/// deadline/teardown machinery is shared with [Wiring].
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

    /// Drops repeat references, keeping first-seen order, so a resource reachable twice (owned
    /// and auto-collected, or a duplicated fan-out peer) is retained once and shut down once -
    /// a plain [AutoCloseable] with a non-idempotent close is never double-closed, and the
    /// shutdown deadline is not spent on repeats.
    static List<AutoCloseable> dedupByIdentity(List<AutoCloseable> resources) {
        var seen = Collections.newSetFromMap(new IdentityHashMap<AutoCloseable, Boolean>());
        var deduped = new ArrayList<AutoCloseable>(resources.size());
        for (var resource : resources) {
            if (seen.add(resource)) {
                deduped.add(resource);
            }
        }
        return Collections.unmodifiableList(deduped);
    }

    static PipelineHandle subscription(
            PipelineHandle sourceSubscription,
            List<AutoCloseable> resources,
            String sourceClass,
            TerminalShape terminalShape,
            String terminalClass,
            boolean ownsDeclared) {
        return new PipelineSubscription(
                sourceSubscription, resources, sourceClass, terminalShape, terminalClass, ownsDeclared);
    }

    static long deadlineNanos(Duration timeout) {
        try {
            return Math.addExact(System.nanoTime(), timeout.toNanos());
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    static Duration remaining(long deadlineNanos) {
        try {
            return Duration.ofNanos(Math.max(0L, Math.subtractExact(deadlineNanos, System.nanoTime())));
        } catch (ArithmeticException e) {
            return Duration.ofNanos(Long.MAX_VALUE);
        }
    }

    /// Runs a stage to completion, recording the first failure so teardown continues.
    static CompletableFuture<Void> settle(
            CompletionStage<Void> stage, AtomicReference<@Nullable Throwable> firstError) {
        return stage.toCompletableFuture().handle((ignored, error) -> {
            if (error != null) {
                firstError.compareAndSet(null, unwrap(error));
            }
            return (Void) null;
        });
    }

    static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException completion && completion.getCause() != null
                ? completion.getCause()
                : error;
    }

    static CompletionStage<Void> closeResource(AutoCloseable resource, Duration timeout) {
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

    /// Best-effort ordered teardown: closes every resource in order even if an earlier step
    /// failed, all within a single shared deadline; the first error is rethrown once every
    /// resource has been attempted.
    static CompletionStage<Void> closeAllInOrder(List<? extends AutoCloseable> ordered, Duration timeout) {
        var deadlineNanos = deadlineNanos(timeout);
        var firstError = new AtomicReference<@Nullable Throwable>();
        var chain = CompletableFuture.<Void>completedFuture(null);
        for (var resource : ordered) {
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

    /// Flushes every [Lifecycle] in order (non-lifecycle resources have nothing to flush), all
    /// within a single shared deadline.
    static CompletionStage<Void> flushAllInOrder(List<? extends AutoCloseable> ordered, Duration timeout) {
        var deadlineNanos = deadlineNanos(timeout);
        var chain = CompletableFuture.<Void>completedFuture(null);
        for (var resource : ordered) {
            if (resource instanceof Lifecycle lifecycle) {
                chain = chain.thenCompose(
                        ignored -> lifecycle.forceFlush(remaining(deadlineNanos)).toCompletableFuture());
            }
        }
        return chain;
    }

    static final class PipelineSubscription implements PipelineHandle {

        /// The source subscription first (stopping ingest, and flushing a buffering source before
        /// its consumers), then the collected resources.
        private final List<AutoCloseable> drainOrder;
        /// The deduplicated collected resources (without the source subscription): the topology view
        /// consumed by [Wiring#validate]/[Wiring#describe] for identity-based cross-pipeline checks.
        private final List<AutoCloseable> resources;
        private final String sourceClass;
        private final TerminalShape terminalShape;
        private final String terminalClass;
        private final boolean ownsDeclared;

        PipelineSubscription(
                PipelineHandle sourceSubscription,
                List<AutoCloseable> resources,
                String sourceClass,
                TerminalShape terminalShape,
                String terminalClass,
                boolean ownsDeclared) {
            var deduped = dedupByIdentity(resources);
            var ordered = new ArrayList<AutoCloseable>(deduped.size() + 1);
            ordered.add(sourceSubscription);
            ordered.addAll(deduped);
            this.drainOrder = Collections.unmodifiableList(ordered);
            this.resources = deduped;
            this.sourceClass = sourceClass;
            this.terminalShape = terminalShape;
            this.terminalClass = terminalClass;
            this.ownsDeclared = ownsDeclared;
            // Register as one owner of every shared resource, so a resource shared with another
            // subscription (e.g. a multi-signal exporter) survives until this subscription shuts down.
            for (var resource : deduped) {
                if (resource instanceof SharedLifecycle shared) {
                    shared.retain();
                }
            }
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return closeAllInOrder(drainOrder, timeout);
        }

        @Override
        public CompletionStage<Void> forceFlush(Duration timeout) {
            return flushAllInOrder(drainOrder, timeout);
        }

        @Override
        public PipelineTopology topology() {
            return new PipelineTopology(terminalShape, terminalClass, ownsDeclared, resources);
        }

        @Override
        public String describe() {
            var sb = new StringBuilder("pipeline:\n");
            sb.append("  source: ").append(sourceClass).append('\n');
            sb.append("  terminal: ").append(terminalShape).append(" (").append(terminalClass).append(")");
            if (terminalShape == TerminalShape.OPAQUE_LAMBDA && !ownsDeclared) {
                sb.append(" [captures invisible - declare closeables via owns()]");
            }
            sb.append('\n');
            if (resources.isEmpty()) {
                sb.append("  resources: none");
            } else {
                sb.append("  resources (").append(resources.size()).append("):");
                for (var resource : resources) {
                    sb.append("\n    - [").append(role(resource)).append("] ")
                            .append(resource.getClass().getName());
                }
            }
            return sb.toString();
        }

        /// The lifecycle role of a drained resource, for [#describe].
        private static String role(AutoCloseable resource) {
            if (resource instanceof SharedLifecycle) {
                return "shared";
            }
            if (resource instanceof Lifecycle) {
                return "lifecycle";
            }
            return "closeable";
        }
    }
}
