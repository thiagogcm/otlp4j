package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.MetricsData;

/// Counts OTLP items inside domain batches for partial-success reporting.
public final class BatchItemCounters {

    private BatchItemCounters() {
    }

    /// Delegates to [MetricsData#dataPointCount()].
    public static long metricDataPoints(MetricsData metrics) {
        return metrics.dataPointCount();
    }
}
