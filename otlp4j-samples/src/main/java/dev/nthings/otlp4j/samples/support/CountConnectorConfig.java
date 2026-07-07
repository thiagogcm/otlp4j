package dev.nthings.otlp4j.samples.support;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import java.util.Objects;

/// The configuration for a [CountConnectorFactory]: the name and description of the derived count
/// metric. Defaults to `otlp4j.connector.span.count`, reproducing the legacy span-count connector's
/// metric name.
///
/// @param metricName  the name of the derived delta-sum metric
/// @param description the metric description
public record CountConnectorConfig(String metricName, String description) implements ComponentConfig {

    /// Validates that the components are present.
    public CountConnectorConfig {
        Objects.requireNonNull(metricName, "metricName");
        Objects.requireNonNull(description, "description");
    }

    /// The default configuration: the `otlp4j.connector.span.count` metric.
    ///
    /// @return the default config
    public static CountConnectorConfig defaults() {
        return new CountConnectorConfig("otlp4j.connector.span.count", "spans observed");
    }
}
