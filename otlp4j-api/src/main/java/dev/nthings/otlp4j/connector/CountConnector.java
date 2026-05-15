package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.util.List;

/// A [Connector] that turns trace and log batches into count metrics.
///
/// It emits `otlp4j.connector.span.count` and `otlp4j.connector.log.record.count` as monotonic,
/// delta-temporality sums into the downstream metrics consumer.
public final class CountConnector extends Connector {

    private static final InstrumentationScope SCOPE =
            new InstrumentationScope("otlp4j-count-connector", "0.1.0", Attributes.empty(), 0);

    public CountConnector(TelemetryConsumer downstream) {
        super(downstream);
    }

    @Override
    public ExportResult consumeTraces(TraceData traces) {
        return downstream()
                .consumeMetrics(countMetric("otlp4j.connector.span.count", traces.spans().size()));
    }

    @Override
    public ExportResult consumeLogs(LogsData logs) {
        return downstream()
                .consumeMetrics(
                        countMetric("otlp4j.connector.log.record.count", logs.logRecords().size()));
    }

    private static MetricsData countMetric(String name, long count) {
        var epochNanos = System.currentTimeMillis() * 1_000_000L;
        var point = new NumberPoint(
                Attributes.empty(), 0L, epochNanos, NumberPoint.longValue(count), 0L);
        var metric = new Metric(
                name,
                "Items observed by CountConnector",
                "{item}",
                new Metric.Sum(List.of(point), Metric.AggregationTemporality.DELTA, true),
                Attributes.empty());
        return new MetricsData(List.of(new MetricsData.ResourceMetrics(
                Resource.EMPTY,
                "",
                List.of(new MetricsData.ScopeMetrics(SCOPE, "", List.of(metric))))));
    }
}
