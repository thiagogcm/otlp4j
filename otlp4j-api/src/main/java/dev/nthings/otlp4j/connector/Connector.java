package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.util.Objects;

/// A terminal consumer that emits derived telemetry into a downstream [TelemetryConsumer].
///
/// Connectors may cross signal types and do not have to forward the signal they consume. Override
/// the `consumeX` methods for signals the connector bridges.
public abstract class Connector implements TelemetryConsumer {

    private final TelemetryConsumer downstream;

    protected Connector(TelemetryConsumer downstream) {
        this.downstream = Objects.requireNonNull(downstream, "downstream");
    }

    /// The consumer this connector emits derived telemetry into.
    protected final TelemetryConsumer downstream() {
        return downstream;
    }
}
