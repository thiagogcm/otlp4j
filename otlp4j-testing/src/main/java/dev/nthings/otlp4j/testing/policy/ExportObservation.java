package dev.nthings.otlp4j.testing.policy;

import dev.nthings.otlp4j.core.result.DeliveryResult;
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
    /// transport. A rejection records failure independently of the number of attempts;
    /// `TIMED_OUT` is a total-budget expiry.
    public enum Outcome {
        ACCEPTED,
        PARTIAL,
        REJECTED,
        TIMED_OUT
    }

    /// Builds an observation, classifying the outcome from the settled caller stage.
    ///
    /// @param attempts         the number of attempts that reached the server
    /// @param interAttemptGaps the gaps between consecutive attempt arrivals
    /// @param totalElapsed     the wall-clock time the single export call took to settle
    /// @param result           the delivery result the stage completed with, or null on a failure
    /// @param failure          the throwable the stage completed with, or null on a returned result
    /// @param retries          the value read from the exporter's retry counter for this export
    /// @return the observation
    public static ExportObservation of(
            int attempts,
            List<Duration> interAttemptGaps,
            Duration totalElapsed,
            @Nullable DeliveryResult result,
            @Nullable Throwable failure,
            int retries) {
        return new ExportObservation(
                attempts,
                interAttemptGaps,
                totalElapsed,
                classify(result, failure),
                retries);
    }

    /// Classifies the settled caller stage without deriving an outcome from the expected retry policy.
    /// A transport exception cannot establish whether a rejection is retryable merely by how many
    /// attempts reached the server. The suite checks retry behavior separately through those arrivals.
    ///
    /// @param result      the delivery result, or null on a failure
    /// @param failure     the throwable, or null on a returned result
    /// @return the neutral outcome
    public static Outcome classify(
            @Nullable DeliveryResult result, @Nullable Throwable failure) {
        if (result != null) {
            return switch (result) {
                case DeliveryResult.Accepted ignored -> Outcome.ACCEPTED;
                case DeliveryResult.Partial ignored -> Outcome.PARTIAL;
                case DeliveryResult.Rejected ignored -> Outcome.REJECTED;
            };
        }
        var cause = unwrap(failure);
        if (cause instanceof TimeoutException) {
            return Outcome.TIMED_OUT;
        }
        if (cause == null) {
            throw new IllegalArgumentException("an export observation requires a result or a failure");
        }
        return Outcome.REJECTED;
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
