package dev.nthings.otlp4j.core.component;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/// A gateway component with a deadline-bounded lifecycle, using the Collector lifecycle vocabulary.
///
/// [#start(ComponentContext)] brings the component live and is bounded by the service start
/// deadline. [#forceFlush(FlushContext)] pushes buffered telemetry downstream without tearing the
/// component down and defaults to a no-op, because only a buffering component has anything to flush.
/// [#shutdown(ShutdownContext)] drains in-flight work and releases resources within its deadline.
/// [#close()] is the try-with-resources idiom: it shuts down within the [#DEFAULT_GRACE_PERIOD] and
/// swallows a drain failure or timeout as a WARN log rather than throwing, since a throw during
/// teardown would mask the primary exception being unwound. Call [#shutdown(ShutdownContext)]
/// directly when the drain outcome must be observed.
public interface Component extends AutoCloseable {

    /// The grace period [#close()] drains within: ten seconds, applied uniformly by every component.
    Duration DEFAULT_GRACE_PERIOD = Duration.ofSeconds(10);

    /// Brings the component live within the context's start budget.
    ///
    /// @param context the ambient start context (metrics handle and clock)
    /// @return a stage that completes when the component is live, or exceptionally on failure
    CompletionStage<Void> start(ComponentContext context);

    /// Pushes buffered telemetry downstream without tearing the component down. The default does
    /// nothing; only a buffering component overrides it.
    ///
    /// @param context the flush context carrying the deadline
    /// @return a stage that completes when flushed
    default CompletionStage<Void> forceFlush(FlushContext context) {
        return CompletableFuture.completedFuture(null);
    }

    /// Drains in-flight work and releases resources within the context's deadline.
    ///
    /// @param context the shutdown context carrying the deadline
    /// @return a stage that completes on clean drain or exceptionally on timeout
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
