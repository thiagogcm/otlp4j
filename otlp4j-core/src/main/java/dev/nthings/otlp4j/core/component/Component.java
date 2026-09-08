package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.result.FlushReport;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/// A gateway component with a deadline-bounded lifecycle, using the Collector lifecycle vocabulary.
///
/// [#start(ComponentContext)] brings the component live and is bounded by the service start
/// deadline. [#flush(FlushContext)] pushes buffered telemetry downstream without tearing the
/// component down and defaults to a no-op, because only a buffering component has anything to flush.
/// [#shutdown(ShutdownContext)] drains in-flight work and releases resources within its deadline.
/// Implementations must make [#shutdown(ShutdownContext)] safe before [#start(ComponentContext)],
/// after a partially failed start, and on repeated calls; shutdown is idempotent.
/// [#close()] is the try-with-resources idiom: it shuts down within the [#DEFAULT_GRACE_PERIOD] and
/// rethrows a drain failure or timeout as an [IllegalStateException]. Call
/// [#shutdown(ShutdownContext)] directly when the drain outcome must be observed as a stage.
public interface Component extends AutoCloseable {

    /// The grace period [#close()] drains within: ten seconds, applied uniformly by every component.
    Duration DEFAULT_GRACE_PERIOD = Duration.ofSeconds(10);

    /// Brings the component live within the context's start budget.
    ///
    /// @param context the ambient start context (metrics handle and clock)
    /// @return a stage that completes when the component is live, or exceptionally on failure
    CompletionStage<Void> start(ComponentContext context);

    /// Drains a snapshot of buffered or in-flight telemetry without tearing the component down.
    /// Reports delivery losses after buffered acceptance; direct delivery outcomes remain on their
    /// original submission stages. The default completes immediately for components with no work
    /// of their own to drain.
    ///
    /// @param context the flush context carrying the deadline
    /// @return a stage that completes with the flush report
    default CompletionStage<FlushReport> flush(FlushContext context) {
        return CompletableFuture.completedFuture(FlushReport.empty());
    }

    /// Drains in-flight work and releases resources within the context's deadline.
    ///
    /// @param context the shutdown context carrying the deadline
    /// @return a stage that completes on clean drain or exceptionally on failure or timeout
    CompletionStage<Void> shutdown(ShutdownContext context);

    /// Synchronous drain within the [#DEFAULT_GRACE_PERIOD] for the try-with-resources idiom. A drain
    /// failure or timeout is rethrown wrapped in an [IllegalStateException] rather than logged, so the
    /// contracts module carries no logging dependency; call [#shutdown(ShutdownContext)] directly when
    /// the drain outcome must be observed without the wrapping.
    @Override
    default void close() {
        try {
            shutdown(new ShutdownContext(DEFAULT_GRACE_PERIOD)).toCompletableFuture().join();
        } catch (CompletionException | CancellationException e) {
            throw new IllegalStateException("graceful shutdown of " + this + " failed or timed out during close()", e);
        }
    }
}
