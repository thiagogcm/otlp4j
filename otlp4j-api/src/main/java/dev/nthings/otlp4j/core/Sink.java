package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// A typed, asynchronous sink for one OTLP signal.
///
/// The four signals expose their own SAMs — [TraceSink], [MetricSink], [LogSink],
/// [ProfileSink] — which is what user code plugs lambdas into. Extending `Sink<T>`
/// directly is useful only when a single type parameter is genuinely needed.
///
/// When a handler only needs success/failure semantics rather than the full
/// [ConsumeResult] vocabulary, the [#accepting(ThrowingConsumer)] and [#fromStage(Function)]
/// factories build a sink from a plain consumer or a `CompletionStage<Void>`-returning function,
/// so callers do not have to return `ConsumeResult.acceptedStage()` by hand. The per-signal SAMs
/// carry the same factories narrowed to their type, e.g. [TraceSink#accepting(ThrowingConsumer)].
/// To report item-level partial success ([ConsumeResult.Partial]) rather than just accept/reject,
/// implement the sink directly and return the [ConsumeResult] yourself.
///
/// @param <T> the OTLP signal carried by this sink
public interface Sink<T> {

    /// Accepts one batch and returns a stage that completes when the batch has been processed.
    CompletionStage<ConsumeResult<T>> consume(T batch);

    /// Builds a sink from a synchronous `action`: returning normally accepts the batch, and any
    /// thrown exception becomes a permanent (non-retryable) [ConsumeResult.Rejected] carrying that
    /// exception as its cause. If the action throws [InterruptedException], the thread interrupt
    /// flag is restored before returning the rejection. `Error`s are not caught and propagate to the
    /// caller.
    static <T> Sink<T> accepting(ThrowingConsumer<? super T> action) {
        Objects.requireNonNull(action, "action");
        return batch -> {
            try {
                action.accept(batch);
            } catch (Exception e) {
                restoreInterrupt(e);
                return CompletableFuture.completedFuture(rejected(e));
            }
            return ConsumeResult.acceptedStage();
        };
    }

    /// Builds a sink from an asynchronous `action` that reports only success or failure: normal
    /// completion of the returned stage accepts the batch, while an exceptional completion (or a
    /// synchronous throw, or a `null` stage) becomes a permanent [ConsumeResult.Rejected] carrying
    /// the failure as its cause. If the failure is an [InterruptedException], the thread interrupt
    /// flag is restored before returning the rejection.
    static <T> Sink<T> fromStage(Function<? super T, ? extends CompletionStage<Void>> action) {
        Objects.requireNonNull(action, "action");
        return batch -> {
            CompletionStage<Void> stage;
            try {
                stage = Objects.requireNonNull(action.apply(batch), "fromStage action returned a null stage");
            } catch (Exception e) {
                restoreInterrupt(e);
                return CompletableFuture.completedFuture(rejected(e));
            }
            return stage.handle((ignored, failure) -> {
                if (failure == null) {
                    return ConsumeResult.<T>accepted();
                }
                var cause = unwrap(failure);
                restoreInterrupt(cause);
                return rejected(cause);
            });
        };
    }

    private static void restoreInterrupt(Throwable failure) {
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /// Maps a (non-null) failure to a permanent rejection whose cause is the original throwable.
    private static <T> ConsumeResult<T> rejected(Throwable failure) {
        var message = failure.getMessage();
        return ConsumeResult.permanentRejected(
                "sink action threw " + failure.getClass().getSimpleName()
                        + (message == null ? "" : ": " + message),
                failure);
    }

    /// Unwraps the [CompletionException] that [CompletionStage#handle] wraps a failure in, so the
    /// rejection carries the real cause rather than the framework wrapper.
    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }
}
