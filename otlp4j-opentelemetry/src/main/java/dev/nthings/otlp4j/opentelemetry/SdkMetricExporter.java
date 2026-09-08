package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.model.MetricsData;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;

final class SdkMetricExporter extends ExportLifecycle<MetricsData> implements MetricExporter {

    private final AggregationTemporalitySelector temporalitySelector;

    SdkMetricExporter(
            TelemetryInput<MetricsData> input, AggregationTemporalitySelector temporalitySelector) {
        super(input);
        this.temporalitySelector = Objects.requireNonNull(temporalitySelector, "temporalitySelector");
    }

    SdkMetricExporter(
            TelemetryInput<MetricsData> input,
            AggregationTemporalitySelector temporalitySelector,
            Duration shutdownTimeout) {
        super(input, shutdownTimeout);
        this.temporalitySelector = Objects.requireNonNull(temporalitySelector, "temporalitySelector");
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        try {
            var snapshot = ListCopy.copy(metrics);
            if (snapshot.isEmpty()) {
                return emptyExport();
            }
            return submit(() -> input().submit(SdkDataMapper.metrics(snapshot)));
        } catch (RuntimeException exception) {
            return CompletableResultCode.ofExceptionalFailure(exception);
        }
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return temporalitySelector.getAggregationTemporality(
                Objects.requireNonNull(instrumentType, "instrumentType"));
    }

    @Override
    public CompletableResultCode flush() {
        return flushInput();
    }

    @Override
    public CompletableResultCode shutdown() {
        return shutdownAdapter();
    }
}
