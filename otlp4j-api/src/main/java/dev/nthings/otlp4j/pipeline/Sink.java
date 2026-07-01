package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// A typed, asynchronous sink for one OTLP signal.
///
/// The four signals expose their own SAMs - [TraceSink], [MetricSink], [LogSink],
/// [ProfileSink] - which is what user code plugs lambdas into. Extending [Sink] directly
/// is useful only when a single type parameter is genuinely needed.
///
/// When a handler only needs success/failure semantics rather than the full
/// [ConsumeResult] vocabulary, the [#accepting(ThrowingConsumer)] and [#fromStage(Function)]
/// factories build a sink from a plain consumer or a [CompletionStage]-returning function,
/// so callers do not have to return [ConsumeResult#acceptedStage()] by hand.
/// To report item-level partial success ([ConsumeResult.Partial]) rather than just accept/reject,
/// implement the sink directly and return the [ConsumeResult] yourself.
///
/// @param <T> the OTLP signal carried by this sink
public interface Sink<T> {

    /// Accepts one batch and returns a stage that completes when the batch has been processed.
    ///
    /// @param batch the signal batch
    /// @return a stage that completes when processed
    CompletionStage<ConsumeResult<T>> consume(T batch);

    /// Builds a sink from a synchronous action: normal return accepts the batch; a thrown exception
    /// becomes a permanent [ConsumeResult.Rejected] with that exception as its cause, unwrapped from
    /// [CompletionException]. An [InterruptedException] restores the interrupt flag. An [Error] propagates.
    ///
    /// @param action the synchronous action
    /// @param <T>    the signal type
    /// @return a new [Sink]
    static <T> Sink<T> accepting(ThrowingConsumer<? super T> action) {
        Objects.requireNonNull(action, "action");
        return batch -> {
            try {
                action.accept(batch);
            } catch (Throwable e) {
                return CompletableFuture.completedFuture(rejected(e));
            }
            return ConsumeResult.acceptedStage();
        };
    }

    /// Builds a sink from an async action returning a [CompletionStage]: normal completion accepts
    /// the batch; exceptional completion, a synchronous throw, or a null stage becomes a permanent
    /// [ConsumeResult.Rejected] with the failure as its cause, unwrapped from [CompletionException].
    /// An [InterruptedException] restores the interrupt flag. An [Error] propagates.
    ///
    /// @param action the async action
    /// @param <T>    the signal type
    /// @return a new [Sink]
    static <T> Sink<T> fromStage(Function<? super T, ? extends CompletionStage<Void>> action) {
        Objects.requireNonNull(action, "action");
        return batch -> {
            CompletionStage<Void> stage;
            try {
                stage = Objects.requireNonNull(action.apply(batch), "fromStage action returned a null stage");
            } catch (Throwable e) {
                return CompletableFuture.completedFuture(rejected(e));
            }
            return stage.handle((@Nullable Void ignored, @Nullable Throwable failure) -> {
                if (failure == null) {
                    return ConsumeResult.<T>accepted();
                }
                return rejected(failure);
            });
        };
    }

    private static void restoreInterrupt(Throwable failure) {
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /// Maps a failure to a permanent rejection, rethrowing [Error].
    private static <T> ConsumeResult<T> rejected(Throwable failure) {
        var cause = unwrap(failure);
        if (cause instanceof Error error) {
            throw error;
        }
        restoreInterrupt(cause);
        @Nullable String message = cause.getMessage();
        return ConsumeResult.permanent(
                "sink action threw " + cause.getClass().getSimpleName()
                        + (message == null ? "" : ": " + message),
                cause);
    }

    /// Unwraps [CompletionException] to expose the real cause.
    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }
}
