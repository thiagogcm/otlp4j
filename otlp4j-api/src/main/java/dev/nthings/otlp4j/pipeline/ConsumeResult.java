package dev.nthings.otlp4j.pipeline;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// The outcome of feeding one batch of signal `T` to a [Consumer].
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

    /// The whole batch was rejected; `cause` may be `null` when no exception is associated.
    record Rejected<T>(String message, Throwable cause) implements ConsumeResult<T> {

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

    /// Returns a [Rejected] result with no associated exception.
    static <T> ConsumeResult<T> rejected(String message) {
        return new Rejected<>(message, null);
    }

    /// Returns a [Rejected] result wrapping the given throwable.
    static <T> ConsumeResult<T> rejected(String message, Throwable cause) {
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
        Rejected<T> firstRejected = null;
        long maxRejected = 0;
        StringBuilder messages = null;
        for (var peer : peers) {
            switch (peer) {
                case Rejected<T> r -> {
                    if (firstRejected == null) {
                        firstRejected = r;
                    }
                    messages = appendMessage(messages, r.message());
                }
                case Partial<T> p -> {
                    if (p.rejectedItems() > maxRejected) {
                        maxRejected = p.rejectedItems();
                    }
                    messages = appendMessage(messages, p.message());
                }
                case Accepted<T> ignored -> {}
            }
        }
        if (firstRejected != null) {
            return new Rejected<>(messages == null ? firstRejected.message() : messages.toString(), firstRejected.cause());
        }
        if (maxRejected == 0) {
            return accepted();
        }
        return new Partial<>(maxRejected, messages == null ? "" : messages.toString());
    }

    /// Combines two results from **sequential** stages of the same pipeline (stage A then stage B):
    ///
    ///   - If `first` is [Rejected], it is returned (the batch never reached stage B).
    ///   - Otherwise rejection counts **add** — different items can be rejected at different stages.
    ///   - Non-empty messages are concatenated.
    static <T> ConsumeResult<T> sequentialMerge(ConsumeResult<T> first, ConsumeResult<T> second) {
        if (first instanceof Rejected<T>) {
            return first;
        }
        if (second instanceof Rejected<T>) {
            return second;
        }
        long rejected =
                (first instanceof Partial<T> p1 ? p1.rejectedItems() : 0L)
                + (second instanceof Partial<T> p2 ? p2.rejectedItems() : 0L);
        var msg1 = first instanceof Partial<T> p1 ? p1.message() : "";
        var msg2 = second instanceof Partial<T> p2 ? p2.message() : "";
        var combined =
                msg1.isEmpty() ? msg2
                        : msg2.isEmpty() ? msg1
                                : msg1 + "; " + msg2;
        return rejected == 0 ? accepted() : new Partial<>(rejected, combined);
    }

    private static StringBuilder appendMessage(StringBuilder buf, String msg) {
        if (msg == null || msg.isEmpty()) {
            return buf;
        }
        if (buf == null) {
            return new StringBuilder(msg);
        }
        return buf.append("; ").append(msg);
    }
}
