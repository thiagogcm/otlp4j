package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.AsyncTransform;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A signal-changing bridge: consumes batches of one signal and emits derived batches of another
/// (or the same) signal to one or more downstream sinks - the general form of the Collector's
/// connector. The bundled span-count and log-record-count connectors (see [Connectors]) are
/// instances; extend this class to write your own.
///
/// A connector may fan its derived batch to several downstreams: the delivery path is a
/// [FanOut] over the downstream list, so N downstreams get real per-peer parallelism and their
/// results merge with FanOut semantics (worst-case rejection). This has one deliberate consequence.
/// With a single downstream the connector's own try/catch runs and a downstream [Error] is rethrown
/// (fail-fast on a programming bug in the one downstream). With two or more downstreams the fan
/// isolates each peer, so a peer `Error` becomes a captured rejection governed by the
/// [FailurePolicy] rather than aborting delivery to the healthy peers. Both behaviors are correct
/// for their shape; see the [#Connector(String,List,FailurePolicy)] constructor.
///
/// Subclasses implement one method, [#derive], which maps an input batch to zero or one
/// downstream batch synchronously. The base class owns everything else and keeps it correct by
/// construction: delivery and failure normalization, the [FailurePolicy] contract, and the
/// lifecycle cascade. `consume`, `retain`, `shutdown`, and `forceFlush` are final because a
/// subclass override could silently break the retain/shutdown balance that keeps shared
/// downstreams reference-counted; connector-local state is drained through the [#onShutdown] and
/// [#onForceFlush] hooks instead, which run before the downstream cascade so a buffering
/// connector flushes into a still-open downstream.
///
/// The connector owns its downstreams: attaching the connector to a pipeline drains every
/// downstream automatically with no `Stage.owns(...)` declaration, and the pipeline's retain is
/// propagated to each downstream that is itself a [SharedLifecycle].
///
/// A connector needing asynchronous derivation or multi-batch emission implements [Sink]
/// directly instead (composing an [AsyncTransform] before the connector covers most async
/// pre-steps).
///
/// @param <I> the input signal consumed by this connector
/// @param <O> the derived signal emitted downstream
public abstract class Connector<I, O> implements Sink<I>, SharedLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Connector.class);

    private final String name;
    /// The downstream sinks, one or many; the source of truth for the lifecycle cascade (a
    /// [FanOut] is not a [Lifecycle], so cascading over the raw list is the only correct way to
    /// drain a fan of exporter facets).
    private final List<Sink<? super O>> downstreams;
    /// The merged delivery path: [FanOut#of] over [#downstreams] (the bare peer for one, a real
    /// [FanOut] for many).
    private final Sink<? super O> delivery;
    private final FailurePolicy policy;

    /// Creates a best-effort connector (see [FailurePolicy#BEST_EFFORT]): a failure of the
    /// derived delivery never fails the input batch.
    ///
    /// @param name       the connector name, used in log and rejection messages
    /// @param downstream the sink receiving derived batches
    protected Connector(String name, Sink<? super O> downstream) {
        this(name, List.of(Objects.requireNonNull(downstream, "downstream")), FailurePolicy.BEST_EFFORT);
    }

    /// Creates a connector with the given failure policy.
    ///
    /// @param name       the connector name, used in log and rejection messages
    /// @param downstream the sink receiving derived batches
    /// @param policy     how a derived-delivery failure reflects on the input batch
    protected Connector(String name, Sink<? super O> downstream, FailurePolicy policy) {
        this(name, List.of(Objects.requireNonNull(downstream, "downstream")), policy);
    }

    /// Fans the derived batch to every downstream, merging their results with [FanOut] semantics
    /// (worst-case rejection). At least one downstream is required.
    ///
    /// A peer that throws or fails when there are two or more downstreams is isolated by the fan and
    /// its failure becomes a rejection governed by `policy`, unlike the single-downstream form which
    /// propagates an [Error] and restores an interrupt flag. See the class-level divergence note.
    ///
    /// @param name        the connector name, used in log and rejection messages
    /// @param downstreams the sinks receiving derived batches (at least one)
    /// @param policy      how a merged derived-delivery failure reflects on the input batch
    protected Connector(String name, List<? extends Sink<? super O>> downstreams, FailurePolicy policy) {
        this.name = Objects.requireNonNull(name, "name");
        this.downstreams = List.copyOf(downstreams);   // List.copyOf rejects null elements
        if (this.downstreams.isEmpty()) {
            throw new IllegalArgumentException(name + ": a connector needs at least one downstream");
        }
        this.delivery = FanOut.of(this.downstreams);   // one -> the bare peer, many -> a real FanOut
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    /// Derives zero or one downstream batch from the input. Returning null emits nothing and
    /// accepts the input. Synchronous and non-blocking; a throw is governed by the
    /// [FailurePolicy] like any other derived-delivery failure.
    ///
    /// @param batch the input batch
    /// @return the derived batch, or null to emit nothing
    protected abstract @Nullable O derive(I batch);

    /// The connector name used in log and rejection messages.
    ///
    /// @return the connector name
    protected final String name() {
        return name;
    }

    @Override
    public final CompletionStage<ConsumeResult> consume(I batch) {
        @Nullable O derived;
        try {
            derived = derive(batch);
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(applyPolicy(rejected("derive threw", e)));
        }
        if (derived == null) {
            return ConsumeResult.acceptedStage();
        }
        // Normalize a throw or failed stage so applyPolicy governs the input.
        CompletionStage<ConsumeResult> downstreamStage;
        try {
            downstreamStage = Objects.requireNonNull(
                    delivery.consume(derived), "downstream returned a null stage");
        } catch (Throwable e) {
            downstreamStage = CompletableFuture.completedFuture(rejected("downstream threw", e));
        }
        return downstreamStage
                .exceptionally(t -> rejected("downstream failed", t))
                .thenApply(this::applyPolicy);
    }

    @Override
    public final void retain() {
        // Propagate ownership so a shared downstream stays reference-counted when reached only through us.
        for (var d : downstreams) {
            if (d instanceof SharedLifecycle shared) {
                shared.retain();
            }
        }
    }

    @Override
    public final void release() {
        // Mirror retain()'s propagation: drop one owner's stake on each shared downstream without draining.
        for (var d : downstreams) {
            if (d instanceof SharedLifecycle shared) {
                shared.release();
            }
        }
    }

    @Override
    public final CompletionStage<Void> shutdown(Duration timeout) {
        return onShutdown(timeout).thenCompose(ignored -> cascade(Lifecycle::shutdown, timeout));
    }

    @Override
    public final CompletionStage<Void> forceFlush(Duration timeout) {
        return onForceFlush(timeout).thenCompose(ignored -> cascade(Lifecycle::forceFlush, timeout));
    }

    /// Drains or flushes every [Lifecycle] downstream under one deadline; non-Lifecycle peers are
    /// skipped. The single-downstream case passes the peer stage through unwrapped so its failure
    /// nesting is byte-identical to the pre-multi-downstream direct ternary; only the true
    /// multi-downstream case introduces the `allOf` wrapper, and that case did not exist before.
    private CompletionStage<Void> cascade(BiFunction<Lifecycle, Duration, CompletionStage<Void>> op, Duration t) {
        var lifecycles = downstreams.stream()
                .filter(Lifecycle.class::isInstance)
                .map(Lifecycle.class::cast)
                .toList();
        if (lifecycles.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (lifecycles.size() == 1) {
            return op.apply(lifecycles.get(0), t);   // unwrapped passthrough
        }
        var futures = lifecycles.stream()
                .map(d -> op.apply(d, t).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    /// Drains connector-local state on shutdown, before the downstream cascade. Default: no-op.
    ///
    /// @param timeout the shutdown deadline
    /// @return a stage completing when local state is drained
    protected CompletionStage<Void> onShutdown(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Flushes connector-local state on forceFlush, before the downstream cascade. Default: no-op.
    ///
    /// @param timeout the flush deadline
    /// @return a stage completing when local state is flushed
    protected CompletionStage<Void> onForceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Permanent rejection from a derived-delivery failure, normalized by
    /// [ConsumeResult#permanentFrom] (an [Error] is rethrown, not swallowed past `applyPolicy`).
    private ConsumeResult rejected(String verb, Throwable failure) {
        return ConsumeResult.permanentFrom(name + " " + verb, failure);
    }

    /// Maps the derived-delivery result onto the input result; logs failures, and under
    /// [FailurePolicy#FAIL] propagates them as a `Rejected` on the input.
    private ConsumeResult applyPolicy(ConsumeResult downstreamResult) {
        switch (downstreamResult) {
            case ConsumeResult.Accepted _ -> {
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Partial(var rejectedItems, var message) -> {
                log.warn("{} downstream partial_success: {} rejected items, msg={}", name, rejectedItems, message);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.retryable(
                            name + " derived batch partially rejected: " + rejectedItems + " item(s): " + message);
                }
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Rejected(var retryable, var message, var cause, var retryAfter) -> {
                log.warn("{} derived delivery rejected: {}", name, message, cause);
                if (policy == FailurePolicy.FAIL) {
                    var forwarded = name + " derived batch rejected: " + message;
                    if (!retryable) {
                        return ConsumeResult.permanent(forwarded, cause);
                    }
                    return retryAfter != null
                            ? ConsumeResult.throttled(retryAfter, forwarded, cause)
                            : ConsumeResult.retryable(forwarded, cause);
                }
                return ConsumeResult.accepted();
            }
        }
    }
}
