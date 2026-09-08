package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.TracesData;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Objects;

/**
 * Factory methods for adapting in-process otlp4j inputs to OpenTelemetry SDK exporters.
 *
 * <p>Adapters borrow their inputs and never close the shared input runtime. Each adapter shutdown
 * uses a ten-second monotonic drain budget; a timeout fails the SDK result without cancelling work
 * owned by the input.
 */
public final class SdkExporters {

    private SdkExporters() {}

    /**
     * Creates a span exporter that submits SDK snapshots to {@code input}.
     *
     * <p>The returned exporter borrows the input. Shutting down the exporter drains that input's
     * accepted work for at most ten seconds (including the final input flush), but does not close
     * or otherwise stop the input runtime. A timeout fails the SDK result without cancelling work
     * owned by the shared input.
     *
     * @param input the traces input to borrow
     * @return an OpenTelemetry SDK span exporter
     */
    public static SpanExporter spans(TelemetryInput<TracesData> input) {
        return new SdkSpanExporter(Objects.requireNonNull(input, "input"));
    }

    /**
     * Creates a metrics exporter using cumulative aggregation temporality for every instrument.
     *
     * @param input the metrics input to borrow
     * @return an OpenTelemetry SDK metrics exporter
     */
    public static MetricExporter metrics(TelemetryInput<MetricsData> input) {
        return metrics(input, AggregationTemporalitySelector.alwaysCumulative());
    }

    /**
     * Creates a metrics exporter using the supplied official SDK temporality selector.
     *
     * @param input the metrics input to borrow
     * @param temporalitySelector the SDK selector used by metric readers
     * @return an OpenTelemetry SDK metrics exporter
     */
    public static MetricExporter metrics(
            TelemetryInput<MetricsData> input,
            AggregationTemporalitySelector temporalitySelector) {
        return new SdkMetricExporter(
                Objects.requireNonNull(input, "input"),
                Objects.requireNonNull(temporalitySelector, "temporalitySelector"));
    }

    /**
     * Creates a log exporter that submits SDK snapshots to {@code input}.
     *
     * @param input the logs input to borrow
     * @return an OpenTelemetry SDK log exporter
     */
    public static LogRecordExporter logs(TelemetryInput<LogsData> input) {
        return new SdkLogRecordExporter(Objects.requireNonNull(input, "input"));
    }
}
