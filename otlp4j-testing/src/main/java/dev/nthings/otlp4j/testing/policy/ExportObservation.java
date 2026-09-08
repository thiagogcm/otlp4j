package dev.nthings.otlp4j.testing.policy;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;

/// What a single programmed export was observed to do: how many attempts reached the server, the gaps
/// between consecutive attempt arrivals, the caller-observed total elapsed time, the neutral outcome,
/// and the number of retry attempts the real client emitted through the exporter's retry counter.
/// Attempts and gaps come from the programmed server's recorded per-attempt arrival timestamps; the
/// outcome is derived from the settled caller stage by the classification rule in [#classify]; the
/// retry count is read from the exporter's `otlp4j.exporter.export.retries` instrument, which the real
/// wire client feeds from its retry listener, so a case can pin that wiring end to end.
///
/// @param attempts         the number of attempts that reached the server
/// @param interAttemptGaps the gaps between consecutive attempt arrivals, one fewer than `attempts`
/// @param totalElapsed     the wall-clock time the single export call took to settle
/// @param outcome          the neutral outcome of the export
/// @param retries          the count read from the exporter's retry counter for this export
public record ExportObservation(
        int attempts, List<Duration> interAttemptGaps, Duration totalElapsed, Outcome outcome, int retries) {

    /// Copies the gaps defensively.
    public ExportObservation {
        interAttemptGaps = List.copyOf(interAttemptGaps);
    }

    /// The neutral outcome of an export, agreed by every seam so a case reads the same value on any
    /// transport. `RETRYABLE_REJECTED` and `PERMANENT_REJECTED` are distinguished by whether the retries
    /// were exhausted (see [#classify]); `TIMED_OUT` is a total-budget expiry.
    public enum Outcome {
        ACCEPTED,
        PARTIAL,
        RETRYABLE_REJECTED,
        PERMANENT_REJECTED,
        TIMED_OUT
    }

    /// Builds an observation, classifying the outcome from the settled caller stage.
    ///
    /// @param attempts         the number of attempts that reached the server
    /// @param interAttemptGaps the gaps between consecutive attempt arrivals
    /// @param totalElapsed     the wall-clock time the single export call took to settle
    /// @param result           the delivery result the stage completed with, or null on a failure
    /// @param failure          the throwable the stage completed with, or null on a returned result
    /// @param maxAttempts      the retry policy's attempt count, an input to the rejection classification
    /// @param retries          the value read from the exporter's retry counter for this export
    /// @return the observation
    public static ExportObservation of(
            int attempts,
            List<Duration> interAttemptGaps,
            Duration totalElapsed,
            @Nullable DeliveryResult result,
            @Nullable Throwable failure,
            int maxAttempts,
            int retries) {
        return new ExportObservation(
                attempts,
                interAttemptGaps,
                totalElapsed,
                classify(result, failure, attempts, maxAttempts),
                retries);
    }

    /// The protocol-agnostic outcome classification rule. It reads the returned result or the
    /// unwrapped failure type and,
    /// for a plain transport rejection, the recorded attempt count relative to the policy's attempt
    /// count, never a transport-specific exception type.
    ///
    /// 1. A returned [DeliveryResult] maps [DeliveryResult.Accepted] to `ACCEPTED`, [DeliveryResult.Partial]
    ///    to `PARTIAL`, and a [DeliveryResult.Rejected] to `RETRYABLE_REJECTED` or `PERMANENT_REJECTED` by
    ///    its disposition. The rule remains total for a transport that returns a rejection.
    /// 2. An exceptional stage whose unwrapped cause is a [TimeoutException] is `TIMED_OUT`; the outer
    ///    total-budget `orTimeout` is the only source of that type on these paths.
    /// 3. Any other exceptional stage is a transport rejection. If `attempts == maxAttempts` the error
    ///    was retryable and the attempts were exhausted, so it is `RETRYABLE_REJECTED`; if
    ///    `attempts < maxAttempts` the retry predicate stopped a non-retryable error at the first
    ///    attempt, so it is `PERMANENT_REJECTED`. This matches the retrying client's own attempt
    ///    semantics and holds identically for both protocols.
    ///
    /// @param result      the delivery result, or null on a failure
    /// @param failure     the throwable, or null on a returned result
    /// @param attempts    the recorded attempt count
    /// @param maxAttempts the policy's attempt count
    /// @return the neutral outcome
    public static Outcome classify(
            @Nullable DeliveryResult result, @Nullable Throwable failure, int attempts, int maxAttempts) {
        if (result != null) {
            return switch (result) {
                case DeliveryResult.Accepted ignored -> Outcome.ACCEPTED;
                case DeliveryResult.Partial ignored -> Outcome.PARTIAL;
                case DeliveryResult.Rejected rejected ->
                        rejected.disposition() == Disposition.RETRYABLE
                                ? Outcome.RETRYABLE_REJECTED
                                : Outcome.PERMANENT_REJECTED;
            };
        }
        var cause = unwrap(failure);
        if (cause instanceof TimeoutException) {
            return Outcome.TIMED_OUT;
        }
        return attempts >= maxAttempts ? Outcome.RETRYABLE_REJECTED : Outcome.PERMANENT_REJECTED;
    }

    /// Unwraps a [CompletionException] to its cause, so the classification reads the transport failure
    /// rather than the join wrapper.
    private static @Nullable Throwable unwrap(@Nullable Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }
}
