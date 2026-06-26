package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

/// The outcome of feeding one batch of signal `T` to a sink.
///
/// Three variants map directly to OTLP partial-success semantics: [Accepted] is full success,
/// [Partial] reports an item-level rejection count plus a free-form message, and [Rejected]
/// reports whole-batch failure. The `<T>` parameter prevents accidentally merging rejection
/// counts across signals — OTLP carries `rejected_spans`, `rejected_log_records`,
/// `rejected_data_points` and `rejected_profiles` as four independent fields for the same reason.
///
/// @param <T> the OTLP signal this result describes
public sealed interface ConsumeResult<T> permits ConsumeResult.Accepted, ConsumeResult.Partial, ConsumeResult.Rejected {

    /// The downstream accepted every item.
    record Accepted<T>() implements ConsumeResult<T> {

        private static final Accepted<?> INSTANCE = new Accepted<>();

        @SuppressWarnings("unchecked")
        public static <T> Accepted<T> instance() {
            return (Accepted<T>) INSTANCE;
        }
    }

    /// The downstream rejected `rejectedItems` items; `message` explains why.
    ///
    /// `rejectedItems` is always strictly positive — a zero-rejection partial is just [Accepted].
    record Partial<T>(long rejectedItems, String message) implements ConsumeResult<T> {

        public Partial {
            if (rejectedItems < 1) {
                throw new IllegalArgumentException("rejectedItems must be >= 1, got " + rejectedItems);
            }
            message = message == null ? "" : message;
        }
    }

    /// The whole batch was rejected.
    ///
    /// The presence of `cause` carries retry intent in the gRPC transport: a `null` cause
    /// means the rejection is **transient** and the sender should retry (mapped to gRPC
    /// `UNAVAILABLE`), while a non-null `cause` means the rejection is **permanent** and must not
    /// be retried (mapped to gRPC `INTERNAL`). Prefer the [#retryableRejected(String)] and
    /// [#permanentRejected(String, Throwable)] factories to make that intent explicit.
    record Rejected<T>(String message, @Nullable Throwable cause) implements ConsumeResult<T> {

        public Rejected {
            message = message == null ? "" : message;
        }
    }

    /// Returns a shared [Accepted] result.
    static <T> ConsumeResult<T> accepted() {
        return Accepted.instance();
    }

    /// Returns a [Partial] result with the given rejection count and message.
    static <T> ConsumeResult<T> partial(long rejected, String message) {
        return new Partial<>(rejected, message);
    }

    /// A whole-batch rejection the sender SHOULD retry (transient, e.g. a full queue). Maps to gRPC
    /// `UNAVAILABLE`. Prefer this over the [#rejected(String)] alias when retry intent is known.
    static <T> ConsumeResult<T> retryableRejected(String message) {
        return new Rejected<>(message, null);
    }

    /// A whole-batch rejection the sender MUST NOT retry (permanent, e.g. a policy or validation
    /// fault). The required non-null `cause` maps it to gRPC `INTERNAL` rather than `UNAVAILABLE`.
    static <T> ConsumeResult<T> permanentRejected(String message, Throwable cause) {
        return rejected(message, Objects.requireNonNull(cause, "cause"));
    }

    /// Low-level alias for [#retryableRejected(String)].
    static <T> ConsumeResult<T> rejected(String message) {
        return retryableRejected(message);
    }

    /// Low-level [Rejected] factory forwarding `cause` verbatim (null retries, non-null does not).
    /// Prefer [#retryableRejected(String)] / [#permanentRejected(String, Throwable)] when intent is
    /// known; use this only to forward a cause decided elsewhere.
    static <T> ConsumeResult<T> rejected(String message, @Nullable Throwable cause) {
        return new Rejected<>(message, cause);
    }

    /// Completed stage shorthand for [#accepted()].
    static <T> CompletionStage<ConsumeResult<T>> acceptedStage() {
        return CompletableFuture.completedFuture(accepted());
    }

    /// Combines results from `peers` that all received the **same** batch in parallel
    /// (fan-out semantics):
    ///
    ///   - Any [Rejected] wins (the batch failed for at least one peer).
    ///   - Otherwise the worst-case [Partial#rejectedItems] across peers is reported — sums would
    ///     overstate the rejection from the original sender's viewpoint, since all peers saw the
    ///     same input.
    ///   - Non-empty messages are concatenated with `"; "` for diagnostics.
    static <T> ConsumeResult<T> fanOutMerge(List<ConsumeResult<T>> peers) {
        if (peers.isEmpty()) {
            return accepted();
        }
        @Nullable Rejected<T> firstRejected = null;
        @Nullable Throwable permanentCause = null;
        var maxRejected = 0L;
        @Nullable StringBuilder messages = null;
        for (var peer : peers) {
            switch (peer) {
                case Rejected<T> r -> {
                    if (firstRejected == null) {
                        firstRejected = r;
                    }
                    if (r.cause() != null) {
                        permanentCause = r.cause();
                    }
                    messages = appendMessage(messages, r.message());
                }
                case Partial<T>(var rejectedItems, var message) -> {
                    if (rejectedItems > maxRejected) {
                        maxRejected = rejectedItems;
                    }
                    messages = appendMessage(messages, message);
                }
                case Accepted<T> _ -> {}
            }
        }
        if (firstRejected != null) {
            var cause = permanentCause != null ? permanentCause : firstRejected.cause();
            return new Rejected<>(messages == null ? firstRejected.message() : messages.toString(), cause);
        }
        if (maxRejected == 0) {
            return accepted();
        }
        return new Partial<>(maxRejected, messages == null ? "" : messages.toString());
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
