package dev.nthings.otlp4j.model;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

/// The outcome of feeding one batch to a sink.
///
/// Three variants map directly to OTLP partial-success semantics: [Accepted] for full success,
/// [Partial] with an item-level rejection count and message, and [Rejected] for whole-batch
/// failure. Input safety is carried by `Sink<T>`; the result itself is signal-agnostic.
public sealed interface ConsumeResult permits ConsumeResult.Accepted, ConsumeResult.Partial, ConsumeResult.Rejected {

    /// The downstream accepted every item.
    record Accepted() implements ConsumeResult {

        private static final Accepted INSTANCE = new Accepted();
    }

    /// The downstream rejected `rejectedItems` items; `message` explains why.
    ///
    /// `rejectedItems` is always strictly positive - a zero-rejection partial is just [Accepted].
    record Partial(long rejectedItems, String message) implements ConsumeResult {

        public Partial {
            if (rejectedItems < 1) {
                throw new IllegalArgumentException("rejectedItems must be >= 1, got " + rejectedItems);
            }
            message = message == null ? "" : message;
        }
    }

    /// The whole batch was rejected.
    ///
    /// `retryable` states retry intent: a retryable rejection maps to gRPC `UNAVAILABLE` / HTTP
    /// `503`, a permanent one to gRPC `INTERNAL` / HTTP `500`. `cause` is optional diagnostics,
    /// orthogonal to retryability and present or absent on either disposition. `retryAfter`,
    /// valid only on a retryable rejection, is an optional backpressure hint - how long the
    /// sender should wait before retrying; the transports emit it on the wire (gRPC
    /// `grpc-retry-pushback-ms` trailer, HTTP `Retry-After` header). Prefer the
    /// [#retryable(String)] / [#permanent(String)] / [#throttled(Duration, String)] factories.
    record Rejected(boolean retryable, String message, @Nullable Throwable cause, @Nullable Duration retryAfter)
            implements ConsumeResult {

        public Rejected {
            message = message == null ? "" : message;
            if (retryAfter != null) {
                if (!retryable) {
                    throw new IllegalArgumentException("retryAfter is only valid on a retryable rejection");
                }
                if (retryAfter.isZero() || retryAfter.isNegative()) {
                    throw new IllegalArgumentException("retryAfter must be > 0, got " + retryAfter);
                }
            }
        }
    }

    /// Returns a shared [Accepted] result.
    static ConsumeResult accepted() {
        return Accepted.INSTANCE;
    }

    /// Returns a [Partial] result with the given rejection count and message.
    static ConsumeResult partial(long rejected, String message) {
        return new Partial(rejected, message);
    }

    /// A whole-batch rejection the sender SHOULD retry (transient, e.g. a full queue). Maps to gRPC
    /// `UNAVAILABLE` / HTTP `503`.
    static ConsumeResult retryable(String message) {
        return new Rejected(true, message, null, null);
    }

    /// A retryable rejection carrying a diagnostic `cause`, such as a briefly unreachable downstream.
    /// `cause` may be null when none is available.
    static ConsumeResult retryable(String message, @Nullable Throwable cause) {
        return new Rejected(true, message, cause, null);
    }

    /// A retryable rejection carrying a backpressure hint: the sender should wait `retryAfter`
    /// before retrying. Maps to gRPC `UNAVAILABLE` with a `grpc-retry-pushback-ms` trailer / HTTP
    /// `503` with a `Retry-After` header.
    static ConsumeResult throttled(Duration retryAfter, String message) {
        return new Rejected(true, message, null, retryAfter);
    }

    /// A throttled rejection (see [#throttled(Duration, String)]) carrying a diagnostic `cause`.
    /// `cause` may be null when none is available.
    static ConsumeResult throttled(Duration retryAfter, String message, @Nullable Throwable cause) {
        return new Rejected(true, message, cause, retryAfter);
    }

    /// A whole-batch rejection the sender MUST NOT retry (permanent, e.g. a policy or validation
    /// fault). Maps to gRPC `INTERNAL` / HTTP `500`.
    static ConsumeResult permanent(String message) {
        return new Rejected(false, message, null, null);
    }

    /// A permanent rejection carrying the diagnostic `cause` that decided it. `cause` may be null
    /// when none is available.
    static ConsumeResult permanent(String message, @Nullable Throwable cause) {
        return new Rejected(false, message, cause, null);
    }

    /// Completed stage shorthand for [#accepted()].
    static CompletionStage<ConsumeResult> acceptedStage() {
        return CompletableFuture.completedFuture(accepted());
    }

    /// Combines results from `peers` that all received the **same** batch in parallel
    /// (fan-out semantics):
    ///
    ///   - Any [Rejected] wins (the batch failed for at least one peer). The merge is permanent if
    ///     any rejecting peer is permanent, since retrying cannot satisfy that peer; the first
    ///     non-null peer cause is carried as diagnostics.
    ///   - A retryable merge carries the longest peer [Rejected#retryAfter] - retrying sooner
    ///     cannot satisfy the slowest peer. A permanent merge drops the hint.
    ///   - Otherwise the worst-case [Partial#rejectedItems] across peers is reported - sums would
    ///     overstate the rejection from the original sender's viewpoint, since all peers saw the
    ///     same input.
    ///   - Non-empty messages are concatenated with `"; "` for diagnostics.
    static ConsumeResult fanOutMerge(List<ConsumeResult> peers) {
        if (peers.isEmpty()) {
            return accepted();
        }
        @Nullable Rejected firstRejected = null;
        var anyPermanent = false;
        @Nullable Throwable cause = null;
        @Nullable Duration maxRetryAfter = null;
        var maxRejected = 0L;
        @Nullable StringBuilder messages = null;
        for (var peer : peers) {
            switch (peer) {
                case Rejected r -> {
                    if (firstRejected == null) {
                        firstRejected = r;
                    }
                    if (!r.retryable()) {
                        anyPermanent = true;
                    }
                    if (cause == null && r.cause() != null) {
                        cause = r.cause();
                    }
                    var hint = r.retryAfter();
                    if (hint != null && (maxRetryAfter == null || hint.compareTo(maxRetryAfter) > 0)) {
                        maxRetryAfter = hint;
                    }
                    messages = appendMessage(messages, r.message());
                }
                case Partial(var rejectedItems, var message) -> {
                    if (rejectedItems > maxRejected) {
                        maxRejected = rejectedItems;
                    }
                    messages = appendMessage(messages, message);
                }
                case Accepted _ -> {}
            }
        }
        if (firstRejected != null) {
            return new Rejected(
                    !anyPermanent,
                    messages == null ? firstRejected.message() : messages.toString(),
                    cause,
                    anyPermanent ? null : maxRetryAfter);
        }
        if (maxRejected == 0) {
            return accepted();
        }
        return new Partial(maxRejected, messages == null ? "" : messages.toString());
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
