package dev.nthings.otlp4j.telemetry;

import java.util.function.LongSupplier;

/// Self-observability SPI: gateway components report their own health so an operator can see queue
/// depth, drops, drain failures, admission rejections, and export success/failure. The [#noop]
/// default costs nothing; [LoggingGatewayMetrics] or an OpenTelemetry-SDK adapter turns the
/// instruments into real metrics. Reaches components through the entry-point builders
/// (`OtlpGrpc*`/`OtlpHttp*` `.telemetry(...)`) and
/// [dev.nthings.otlp4j.pipeline.ops.Batcher.Builder#telemetry]; defaults to [#noop].
///
/// Instrument names are namespaced `otlp4j.*` and stable across releases. Implementations must be
/// thread-safe: counters are incremented from receiver, exporter, and drain threads concurrently.
public interface GatewayMetrics {

    /// A monotone counter for `name`; components add deltas as events occur.
    ///
    /// @param name        the `otlp4j.*` instrument name
    /// @param description a one-line human description
    /// @return the counter handle (a no-op handle under [#noop])
    LongCounter counter(String name, String description);

    /// Registers a gauge sampled from `value` at collection time; the returned handle unregisters it
    /// when the component shuts down (so a dead component is not polled forever, preserving the
    /// JVM-isolation posture).
    ///
    /// @param name        the `otlp4j.*` instrument name
    /// @param description a one-line human description
    /// @param value       the live value, polled by the backend
    /// @return a registration to close on component shutdown
    Registration gauge(String name, String description, LongSupplier value);

    /// The shared no-op registry: every instrument does nothing.
    ///
    /// @return the no-op instance
    static GatewayMetrics noop() {
        return NoopGatewayMetrics.INSTANCE;
    }

    /// A monotone counter instrument.
    interface LongCounter {
        /// Adds `delta` (must be greater than or equal to 0).
        ///
        /// @param delta the non-negative increment
        void add(long delta);
    }

    /// A handle that unregisters a previously registered gauge.
    interface Registration extends AutoCloseable {
        /// Unregisters the gauge; idempotent.
        @Override
        void close();
    }
}
