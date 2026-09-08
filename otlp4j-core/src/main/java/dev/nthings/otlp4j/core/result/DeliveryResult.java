package dev.nthings.otlp4j.core.result;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

/// The outcome of feeding one telemetry payload to a consumer.
///
/// Three variants map directly to OTLP partial-success semantics: [Accepted] for full success,
/// [Partial] with an item-level rejection count, and [Rejected] for whole-payload failure. A
/// rejection carries a [Disposition] (the retryable-versus-permanent wire axis), a [DeliveryReason]
/// (the cause, as a reason code), an optional `retryAfter` backpressure hint valid only on a
/// retryable rejection, and a `downstream` marker stating whether the failure originated downstream
/// of the producing component. The downstream concept lives only in that boolean, orthogonal to
/// retryability, so a downstream refusal and a downstream unavailability differ only by disposition
/// and hint. Every variant carries its [Provenance] so a diagnostic can name the producing
/// component and fan-out can preserve it.
public sealed interface DeliveryResult
        permits DeliveryResult.Accepted, DeliveryResult.Partial, DeliveryResult.Rejected {

    /// The provenance of this result: which component produced it, and for which signal.
    ///
    /// @return the provenance
    Provenance provenance();

    /// The downstream accepted every item.
    ///
    /// @param provenance the producing component's provenance
    record Accepted(Provenance provenance) implements DeliveryResult {

        /// Validates that the provenance is present.
        public Accepted {
            requireProvenance(provenance);
        }
    }

    /// The downstream rejected `rejectedItems` items; `message` explains why. Zero is valid when the
    /// server returned a present partial-success block containing a warning but no rejected items.
    ///
    /// @param rejectedItems the number of rejected items, non-negative
    /// @param message       a human-readable explanation, normalized to the empty string when null
    /// @param provenance    the producing component's provenance
    record Partial(long rejectedItems, String message, Provenance provenance) implements DeliveryResult {

        /// Validates the rejection count and normalizes the message.
        public Partial {
            if (rejectedItems < 0) {
                throw new IllegalArgumentException("rejectedItems must be non-negative, got " + rejectedItems);
            }
            message = message == null ? "" : message;
            requireProvenance(provenance);
        }
    }

    /// The whole payload was rejected.
    ///
    /// `disposition` is the retry axis: `RETRYABLE` maps to gRPC `UNAVAILABLE` / HTTP `503`,
    /// `PERMANENT` to gRPC `INTERNAL` / HTTP `500`. `reason` names the cause. `cause` is optional
    /// diagnostics, orthogonal to disposition. `retryAfter`, valid only on a retryable rejection, is
    /// an optional backpressure hint the transports emit on the wire (gRPC `grpc-retry-pushback-ms`
    /// trailer, HTTP `Retry-After` header). `downstream` marks a failure that originated downstream
    /// of the producing component.
    ///
    /// @param disposition the retry axis
    /// @param reason      the cause reason code
    /// @param message     a human-readable explanation, normalized to the empty string when null
    /// @param cause       optional diagnostic cause, may be null
    /// @param retryAfter  optional backpressure hint, valid only when retryable, may be null
    /// @param downstream  whether the failure originated downstream
    /// @param provenance  the producing component's provenance
    record Rejected(
            Disposition disposition,
            DeliveryReason reason,
            String message,
            @Nullable Throwable cause,
            @Nullable Duration retryAfter,
            boolean downstream,
            Provenance provenance) implements DeliveryResult {

        /// Validates the disposition, reason, provenance, and the retry-after invariant.
        public Rejected {
            if (disposition == null) {
                throw new IllegalArgumentException("disposition must be non-null");
            }
            if (reason == null) {
                throw new IllegalArgumentException("reason must be non-null");
            }
            message = message == null ? "" : message;
            if (retryAfter != null) {
                if (disposition != Disposition.RETRYABLE) {
                    throw new IllegalArgumentException("retryAfter is only valid on a retryable rejection");
                }
                if (retryAfter.isZero() || retryAfter.isNegative()) {
                    throw new IllegalArgumentException("retryAfter must be > 0, got " + retryAfter);
                }
            }
            requireProvenance(provenance);
        }
    }

    private static void requireProvenance(Provenance provenance) {
        if (provenance == null) {
            throw new IllegalArgumentException("provenance must be non-null");
        }
    }

    /// A full-success result.
    ///
    /// @param provenance the producing component's provenance
    /// @return an accepted result
    static DeliveryResult accepted(Provenance provenance) {
        return new Accepted(provenance);
    }

    /// A completed stage carrying an [#accepted(Provenance)] result.
    ///
    /// @param provenance the producing component's provenance
    /// @return a completed stage
    static CompletionStage<DeliveryResult> acceptedStage(Provenance provenance) {
        return CompletableFuture.completedFuture(accepted(provenance));
    }

    /// A partial-success result with the given rejection count and message.
    ///
    /// @param rejectedItems the number of rejected items, non-negative
    /// @param message       the explanation
    /// @param provenance    the producing component's provenance
    /// @return a partial result
    static DeliveryResult partial(long rejectedItems, String message, Provenance provenance) {
        return new Partial(rejectedItems, message, provenance);
    }

    /// A whole-payload rejection the sender SHOULD retry, with reason `LOCAL_FAILURE`.
    ///
    /// @param message    the explanation
    /// @param provenance the producing component's provenance
    /// @return a retryable rejection
    static DeliveryResult retryable(String message, Provenance provenance) {
        return retryable(message, null, provenance);
    }

    /// A retryable rejection carrying a diagnostic `cause`, with reason `LOCAL_FAILURE`.
    ///
    /// @param message    the explanation
    /// @param cause      the diagnostic cause, may be null
    /// @param provenance the producing component's provenance
    /// @return a retryable rejection
    static DeliveryResult retryable(String message, @Nullable Throwable cause, Provenance provenance) {
        return new Rejected(
                Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, message, cause, null, false, provenance);
    }

    /// A whole-payload rejection the sender MUST NOT retry, with reason `LOCAL_FAILURE`.
    ///
    /// @param message    the explanation
    /// @param provenance the producing component's provenance
    /// @return a permanent rejection
    static DeliveryResult permanent(String message, Provenance provenance) {
        return permanent(message, null, provenance);
    }

    /// A permanent rejection carrying the diagnostic `cause`, with reason `LOCAL_FAILURE`.
    ///
    /// @param message    the explanation
    /// @param cause      the diagnostic cause, may be null
    /// @param provenance the producing component's provenance
    /// @return a permanent rejection
    static DeliveryResult permanent(String message, @Nullable Throwable cause, Provenance provenance) {
        return new Rejected(
                Disposition.PERMANENT, DeliveryReason.LOCAL_FAILURE, message, cause, null, false, provenance);
    }

    /// A retryable rejection carrying a backpressure hint, with reason `LOCAL_FAILURE`.
    ///
    /// @param retryAfter how long the sender should wait before retrying
    /// @param message    the explanation
    /// @param provenance the producing component's provenance
    /// @return a throttled rejection
    static DeliveryResult throttled(Duration retryAfter, String message, Provenance provenance) {
        return throttled(retryAfter, message, null, provenance);
    }

    /// A throttled rejection carrying a diagnostic `cause`, with reason `LOCAL_FAILURE`.
    ///
    /// @param retryAfter how long the sender should wait before retrying
    /// @param message    the explanation
    /// @param cause      the diagnostic cause, may be null
    /// @param provenance the producing component's provenance
    /// @return a throttled rejection
    static DeliveryResult throttled(
            Duration retryAfter, String message, @Nullable Throwable cause, Provenance provenance) {
        return new Rejected(
                Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, message, cause, retryAfter, false, provenance);
    }

    /// A retryable rejection because the receiver is at capacity, with reason `ADMISSION_REFUSED`
    /// and an optional retry-after hint.
    ///
    /// @param message    the explanation
    /// @param retryAfter an optional backpressure hint, may be null
    /// @param provenance the producing component's provenance
    /// @return an admission-refused rejection
    static DeliveryResult admissionRefused(
            String message, @Nullable Duration retryAfter, Provenance provenance) {
        return new Rejected(
                Disposition.RETRYABLE, DeliveryReason.ADMISSION_REFUSED, message, null, retryAfter, false, provenance);
    }

    /// A retryable rejection because the operation exceeded its deadline, with reason `TIMEOUT`.
    ///
    /// @param message    the explanation
    /// @param provenance the producing component's provenance
    /// @return a timed-out rejection
    static DeliveryResult timedOut(String message, Provenance provenance) {
        return new Rejected(
                Disposition.RETRYABLE, DeliveryReason.TIMEOUT, message, null, null, false, provenance);
    }

    /// A retryable rejection because the gateway is shutting down, with reason `SHUTDOWN`.
    ///
    /// @param message    the explanation
    /// @param provenance the producing component's provenance
    /// @return a shutting-down rejection
    static DeliveryResult shuttingDown(String message, Provenance provenance) {
        return new Rejected(
                Disposition.RETRYABLE, DeliveryReason.SHUTDOWN, message, null, null, false, provenance);
    }

    /// A rejection marked as originating downstream of the producing component, carrying the caller's
    /// chosen disposition and reason. Because downstream is purely the flag, this single factory
    /// covers both the downstream-refused and downstream-unavailable cases; the disposition and any
    /// `retryAfter` distinguish them.
    ///
    /// @param disposition the retry axis
    /// @param reason      the cause reason code
    /// @param message     the explanation
    /// @param cause       the diagnostic cause, may be null
    /// @param retryAfter  an optional backpressure hint, valid only when retryable, may be null
    /// @param provenance  the producing component's provenance
    /// @return a downstream-marked rejection
    static DeliveryResult downstream(
            Disposition disposition,
            DeliveryReason reason,
            String message,
            @Nullable Throwable cause,
            @Nullable Duration retryAfter,
            Provenance provenance) {
        return new Rejected(disposition, reason, message, cause, retryAfter, true, provenance);
    }

    /// Normalizes a delivery failure into a permanent rejection following the framework-wide
    /// conventions: a [java.util.concurrent.CompletionException] is unwrapped to its cause, an
    /// [Error] is rethrown, and an [InterruptedException] restores the thread's interrupt flag. The
    /// rejection message is `prefix + ": " + cause`, its reason `LOCAL_FAILURE`.
    ///
    /// @param prefix     the message prefix naming the failing stage
    /// @param failure    the failure to normalize
    /// @param provenance the producing component's provenance
    /// @return a permanent rejection carrying the unwrapped cause
    static DeliveryResult permanentFrom(String prefix, Throwable failure, Provenance provenance) {
        var cause = failure instanceof CompletionException completion && completion.getCause() != null
                ? completion.getCause()
                : failure;
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new Rejected(
                Disposition.PERMANENT, DeliveryReason.LOCAL_FAILURE, prefix + ": " + cause, cause, null, false,
                provenance);
    }

    /// Combines results from `peers` that all received the **same** payload in parallel (fan-out
    /// semantics), preserving the merged provenance and downstream marker:
    ///
    ///   - Any [Rejected] wins. The merge is permanent if any rejecting peer is permanent, since
    ///     retrying cannot satisfy that peer. The reason, downstream flag, provenance, and cause are
    ///     carried from the first permanent rejecting peer when the merge is permanent, and from the
    ///     first retryable rejecting peer otherwise, so the attributed reason, downstream marker, and
    ///     provenance always describe the peer that actually forced the merged disposition.
    ///   - A retryable merge carries the longest peer `retryAfter`, since retrying sooner cannot
    ///     satisfy the slowest peer. A permanent merge drops the hint.
    ///   - Otherwise the worst-case [Partial] rejection count across peers is reported, since summing
    ///     would overstate the rejection from the original sender's viewpoint.
    ///   - Non-empty messages are concatenated with `"; "` for diagnostics.
    ///
    /// The peer list must be non-empty: a fan-out node always has at least one downstream consumer,
    /// so an empty merge has no provenance to attribute a result to.
    ///
    /// @param peers the per-peer results of one fan-out
    /// @return the merged result
    static DeliveryResult fanOutMerge(List<DeliveryResult> peers) {
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("fanOutMerge requires at least one peer result");
        }
        var fallbackProvenance = peers.get(0).provenance();
        @Nullable Rejected firstPermanent = null;
        @Nullable Rejected firstRetryable = null;
        @Nullable Duration maxRetryAfter = null;
        var maxRejected = 0L;
        var sawPartial = false;
        @Nullable StringBuilder messages = null;
        for (var peer : peers) {
            switch (peer) {
                case Rejected r -> {
                    if (r.disposition() == Disposition.PERMANENT) {
                        if (firstPermanent == null) {
                            firstPermanent = r;
                        }
                    } else if (firstRetryable == null) {
                        firstRetryable = r;
                    }
                    var hint = r.retryAfter();
                    if (hint != null && (maxRetryAfter == null || hint.compareTo(maxRetryAfter) > 0)) {
                        maxRetryAfter = hint;
                    }
                    messages = appendMessage(messages, r.message());
                }
                case Partial(var rejectedItems, var message, var _) -> {
                    sawPartial = true;
                    if (rejectedItems > maxRejected) {
                        maxRejected = rejectedItems;
                    }
                    messages = appendMessage(messages, message);
                }
                case Accepted _ -> {
                    // A fully accepted peer contributes nothing to a merge.
                }
            }
        }
        if (firstPermanent != null || firstRetryable != null) {
            // Attribute the merge to the peer that forced its disposition: the first permanent peer
            // when any peer is permanent, otherwise the first retryable peer. Reason, cause,
            // downstream marker, and provenance all come from that one peer so they stay consistent.
            var permanent = firstPermanent != null;
            var attributed = permanent ? firstPermanent : firstRetryable;
            return new Rejected(
                    permanent ? Disposition.PERMANENT : Disposition.RETRYABLE,
                    attributed.reason(),
                    messages == null ? attributed.message() : messages.toString(),
                    attributed.cause(),
                    permanent ? null : maxRetryAfter,
                    attributed.downstream(),
                    attributed.provenance());
        }
        if (!sawPartial) {
            return new Accepted(fallbackProvenance);
        }
        return new Partial(maxRejected, messages == null ? "" : messages.toString(), fallbackProvenance);
    }

    private static @Nullable StringBuilder appendMessage(@Nullable StringBuilder buf, @Nullable String msg) {
        if (msg == null || msg.isEmpty()) {
            return buf;
        }
        if (buf == null) {
            return new StringBuilder(msg);
        }
        return buf.append("; ").append(msg);
    }
}
