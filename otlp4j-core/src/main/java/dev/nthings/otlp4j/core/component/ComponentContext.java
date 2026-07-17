package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.time.Clock;

/// The ambient context handed to a component at start. It carries the `GatewayMetrics` handle a
/// component reports its own health through, so no component reaches for a static registry, and a
/// logical `Clock` a component reads time from, so time-dependent behaviour is testable with a fixed
/// or offset clock rather than the wall clock.
///
/// @param metrics the self-observability registry, never null (use `GatewayMetrics.noop()` for none)
/// @param clock   the logical clock the component reads time from
public record ComponentContext(GatewayMetrics metrics, Clock clock) {

    /// Validates that neither component is null.
    public ComponentContext {
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must be non-null; use GatewayMetrics.noop()");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must be non-null");
        }
    }
}
