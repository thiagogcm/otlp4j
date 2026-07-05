package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.AsyncTransform;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A signal-changing bridge: consumes batches of one signal and emits derived batches of another
/// (or the same) signal to a single downstream sink - the general form of the Collector's
/// connector. The bundled span-count and log-record-count connectors (see [Connectors]) are
/// instances; extend this class to write your own.
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
/// The connector owns its single downstream: attaching the connector to a pipeline drains the
/// downstream automatically with no `Stage.owns(...)` declaration, and the pipeline's retain is
/// propagated when the downstream is itself a [SharedLifecycle].
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
    private final Sink<? super O> downstream;
    private final FailurePolicy policy;

    /// Creates a best-effort connector (see [FailurePolicy#BEST_EFFORT]): a failure of the
    /// derived delivery never fails the input batch.
    ///
    /// @param name       the connector name, used in log and rejection messages
    /// @param downstream the sink receiving derived batches
    protected Connector(String name, Sink<? super O> downstream) {
        this(name, downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Creates a connector with the given failure policy.
    ///
    /// @param name       the connector name, used in log and rejection messages
    /// @param downstream the sink receiving derived batches
    /// @param policy     how a derived-delivery failure reflects on the input batch
    protected Connector(String name, Sink<? super O> downstream, FailurePolicy policy) {
        this.name = Objects.requireNonNull(name, "name");
        this.downstream = Objects.requireNonNull(downstream, "downstream");
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
                    downstream.consume(derived), "downstream returned a null stage");
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
        if (downstream instanceof SharedLifecycle shared) {
            shared.retain();
        }
    }

    @Override
    public final void release() {
        // Mirror retain()'s propagation: drop one owner's stake on a shared downstream without draining.
        if (downstream instanceof SharedLifecycle shared) {
            shared.release();
        }
    }

    @Override
    public final CompletionStage<Void> shutdown(Duration timeout) {
        return onShutdown(timeout).thenCompose(ignored ->
                downstream instanceof Lifecycle d ? d.shutdown(timeout) : CompletableFuture.completedFuture(null));
    }

    @Override
    public final CompletionStage<Void> forceFlush(Duration timeout) {
        return onForceFlush(timeout).thenCompose(ignored ->
                downstream instanceof Lifecycle d ? d.forceFlush(timeout) : CompletableFuture.completedFuture(null));
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
