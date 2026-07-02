package dev.nthings.otlp4j.pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
