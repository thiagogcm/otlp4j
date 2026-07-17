package dev.nthings.otlp4j.components.connector;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import java.util.Objects;

/// The configuration for a [SpanCountConnectorFactory]: the name and description of the derived count
/// metric. Defaults to `otlp4j.connector.span.count`, the metric name the built-in span-count connector
/// emits.
///
/// @param metricName  the name of the derived delta-sum metric
/// @param description the metric description
public record SpanCountConnectorConfig(String metricName, String description) implements ComponentConfig {

    /// Validates that the components are present.
    public SpanCountConnectorConfig {
        Objects.requireNonNull(metricName, "metricName");
        Objects.requireNonNull(description, "description");
    }

    /// The default configuration: the `otlp4j.connector.span.count` metric.
    ///
    /// @return the default config
    public static SpanCountConnectorConfig defaults() {
        return new SpanCountConnectorConfig("otlp4j.connector.span.count", "spans observed");
    }
}
