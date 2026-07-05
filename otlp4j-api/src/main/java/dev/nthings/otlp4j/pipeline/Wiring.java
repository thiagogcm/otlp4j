package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/// One handle over a whole gateway's wiring: its receivers, its pipeline subscriptions, and any
/// resources hidden from every pipeline. Codifies the shutdown ordering that is otherwise the
/// caller's responsibility:
///
///   1. receivers first, so nothing new is ingested,
///   2. then pipeline subscriptions, draining in-flight work and their collected resources,
///   3. then directly-owned resources (e.g. an exporter reachable only through a lambda),
///
/// all sharing one deadline, best-effort (a failing step never blocks the rest; the first error
/// is rethrown at the end), with repeat references deduplicated across all three phases. A
/// second `shutdown` is a no-op.
///
/// [SharedLifecycle] resources registered via [Builder#owns] are retained once at [Builder#build]
/// and released by the one shutdown, so handing a shared exporter to the wiring AND attaching its
/// facets in member pipelines stays reference-count balanced.
///
/// A `Wiring` participates in refcounting (it retains and releases its owned shared resources)
/// without itself being a [SharedLifecycle], and that asymmetry is intentional: a wiring is a
/// top-level owner (one wiring is one gateway), not a shared resource. Making it `SharedLifecycle`
/// would imply wirings-of-wirings, which is not a use case, so it retains its owned resources once
/// and releases them on its single [#shutdown(Duration)], exactly like any other owner.
///
/// ```java
/// try (var wiring = Wiring.builder()
///         .receiver(receiver)
///         .pipeline(tracesSubscription)
///         .pipeline(metricsSubscription)
///         .owns(exporter)
///         .build()) {
///     // serve until shutdown
/// }
/// ```
public final class Wiring implements Lifecycle {

    private final List<Lifecycle> receivers;
    private final List<PipelineHandle> pipelines;
    private final List<AutoCloseable> owned;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Wiring(Builder builder) {
        this.receivers = List.copyOf(builder.receivers);
        this.pipelines = List.copyOf(builder.pipelines);
        this.owned = PipelineLifecycle.dedupByIdentity(builder.owned);
        // Mirror PipelineSubscription: one retain per shared owned resource, balanced by the one
        // shutdown this wiring issues.
        for (var resource : owned) {
            if (resource instanceof SharedLifecycle shared) {
                shared.retain();
            }
        }
    }

    /// Opens a builder.
    ///
    /// @return a new builder
    public static Builder builder() {
        return new Builder();
    }

    /// Shuts down receivers, then pipelines, then owned resources, sharing one deadline,
    /// best-effort; the first error is rethrown once everything has been attempted. Idempotent:
    /// a second call completes immediately.
    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return PipelineLifecycle.closeAllInOrder(dedupedPhases(), timeout);
    }

    /// Flushes pipelines then owned [Lifecycle] resources, sharing one deadline. Receivers do not
    /// buffer, so they are not flushed.
    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        var ordered = new ArrayList<AutoCloseable>(pipelines.size() + owned.size());
        ordered.addAll(pipelines);
        ordered.addAll(owned);
        return PipelineLifecycle.flushAllInOrder(ordered, timeout);
    }

    /// A human-readable dump of the whole wiring: its receivers, each pipeline's
    /// [PipelineHandle#describe], and its owned resources (shared ones annotated with a structural
    /// owner count). Cheap; intended for logs and tests, never the ingest path.
    ///
    /// @return a human-readable wiring description
    public String describe() {
        var owners = structuralOwnerCounts();
        var sb = new StringBuilder("wiring:\n");
        appendClassList(sb, "receivers", receivers);
        sb.append("  pipelines");
        if (pipelines.isEmpty()) {
            sb.append(": none\n");
        } else {
            sb.append(" (").append(pipelines.size()).append("):\n");
            for (var pipeline : pipelines) {
                sb.append(indent(pipeline.describe())).append('\n');
            }
        }
        sb.append("  owned");
        if (owned.isEmpty()) {
            sb.append(": none");
        } else {
            sb.append(" (").append(owned.size()).append("):");
            for (var resource : owned) {
                sb.append("\n    - ").append(resource.getClass().getName());
                if (resource instanceof SharedLifecycle) {
                    sb.append(" [shared, owners=").append(owners.getOrDefault(resource, 1)).append(']');
                }
            }
        }
        return sb.toString();
    }

    /// Runs cheap structural checks over the declared topology and returns the issues found (empty =
    /// clean). Gated by being an explicit call, never on the ingest path. Honest about its limits: it
    /// cannot see resources captured inside a lambda terminal (see
    /// [ValidationIssue.Kind#OPAQUE_TERMINAL]).
    ///
    /// @return the issues found, empty when the visible topology is clean
    public List<ValidationIssue> validate() {
        var issues = new ArrayList<ValidationIssue>();
        if (!pipelines.isEmpty() && receivers.isEmpty()) {
            issues.add(new ValidationIssue(ValidationIssue.Kind.NO_RECEIVERS,
                    pipelines.size() + " pipeline(s) declared but no receiver ingests into them"));
        }
        if (!receivers.isEmpty() && pipelines.isEmpty()) {
            issues.add(new ValidationIssue(ValidationIssue.Kind.NO_PIPELINES,
                    receivers.size() + " receiver(s) declared but no pipeline drains them"));
        }
        // Count structural attachments of each resource across pipelines, by identity.
        Map<AutoCloseable, Integer> pipelineRefs = new IdentityHashMap<>();
        for (var pipeline : pipelines) {
            var topology = pipeline.topology();
            if ((topology.terminal() == TerminalShape.OPAQUE_LAMBDA
                    || (topology.terminal() == TerminalShape.FANOUT && topology.resources().isEmpty()))
                    && !topology.ownsDeclared()) {
                issues.add(new ValidationIssue(ValidationIssue.Kind.OPAQUE_TERMINAL,
                        "terminal " + topology.terminalClass()
                                + " is opaque; if it captures a closeable, declare it with owns()"));
            }
            for (var resource : topology.resources()) {
                pipelineRefs.merge(resource, 1, Integer::sum);
            }
        }
        // DOUBLE_ATTACHED_UNSHARED: the same non-shared Lifecycle in two pipelines is shut down twice.
        for (var entry : pipelineRefs.entrySet()) {
            var resource = entry.getKey();
            if (entry.getValue() >= 2 && resource instanceof Lifecycle && !(resource instanceof SharedLifecycle)) {
                issues.add(new ValidationIssue(ValidationIssue.Kind.DOUBLE_ATTACHED_UNSHARED,
                        resource.getClass().getName() + " is attached to " + entry.getValue()
                                + " pipelines but is not a SharedLifecycle; it would be shut down more than once"));
            }
        }
        // REDUNDANT_OWNS: an owned resource is also auto-collected by a pipeline.
        for (var resource : owned) {
            if (pipelineRefs.containsKey(resource)) {
                issues.add(new ValidationIssue(ValidationIssue.Kind.REDUNDANT_OWNS,
                        resource.getClass().getName()
                                + " is registered with owns(...) but is also auto-collected by a pipeline"));
            }
        }
        return issues;
    }

    /// Structural owner count per resource: how many deduped references (pipeline attachments plus
    /// the owned registration) point to each, by identity, for [#describe].
    private Map<AutoCloseable, Integer> structuralOwnerCounts() {
        Map<AutoCloseable, Integer> counts = new IdentityHashMap<>();
        for (var pipeline : pipelines) {
            for (var resource : pipeline.topology().resources()) {
                counts.merge(resource, 1, Integer::sum);
            }
        }
        for (var resource : owned) {
            counts.merge(resource, 1, Integer::sum);
        }
        return counts;
    }

    private static void appendClassList(StringBuilder sb, String label, List<? extends AutoCloseable> items) {
        sb.append("  ").append(label);
        if (items.isEmpty()) {
            sb.append(": none\n");
            return;
        }
        sb.append(" (").append(items.size()).append("):");
        for (var item : items) {
            sb.append("\n    - ").append(item.getClass().getName());
        }
        sb.append('\n');
    }

    private static String indent(String block) {
        var sb = new StringBuilder();
        var lines = block.split("\n", -1);
        for (var i = 0; i < lines.length; i++) {
            sb.append("    ").append(lines[i]);
            if (i < lines.length - 1) {
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    /// The three phases in shutdown order, deduplicated by reference identity across phases so a
    /// resource registered twice is shut down once.
    private List<AutoCloseable> dedupedPhases() {
        var ordered = new ArrayList<AutoCloseable>(receivers.size() + pipelines.size() + owned.size());
        ordered.addAll(receivers);
        ordered.addAll(pipelines);
        ordered.addAll(owned);
        return PipelineLifecycle.dedupByIdentity(ordered);
    }

    /// Builder for a [Wiring]; registration order within each phase is the shutdown order.
    public static final class Builder {

        private final List<Lifecycle> receivers = new ArrayList<>();
        private final List<PipelineHandle> pipelines = new ArrayList<>();
        private final List<AutoCloseable> owned = new ArrayList<>();

        private Builder() {
        }

        /// Registers a receiver (or any ingest-side [Lifecycle]) to stop first.
        ///
        /// @param receiver the receiver
        /// @return this builder
        public Builder receiver(Lifecycle receiver) {
            receivers.add(Objects.requireNonNull(receiver, "receiver"));
            return this;
        }

        /// Registers a pipeline subscription to drain after the receivers stop.
        ///
        /// @param handle the pipeline subscription
        /// @return this builder
        public Builder pipeline(PipelineHandle handle) {
            pipelines.add(Objects.requireNonNull(handle, "handle"));
            return this;
        }

        /// Registers a resource hidden from every pipeline (e.g. an exporter reachable only
        /// through a lambda terminal or a connector's downstream), closed last. A
        /// [SharedLifecycle] is retained once at [#build()].
        ///
        /// @param resource the resource
        /// @return this builder
        public Builder owns(AutoCloseable resource) {
            owned.add(Objects.requireNonNull(resource, "resource"));
            return this;
        }

        /// Builds the wiring, retaining every shared owned resource once.
        ///
        /// @return the wiring handle
        public Wiring build() {
            return new Wiring(this);
        }
    }
}
