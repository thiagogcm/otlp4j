package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import java.util.List;
import org.slf4j.Logger;

/// Shared envelope-building and downstream-result handling for count connectors.
final class CountMetrics {

    private static final InstrumentationScope SCOPE =
            new InstrumentationScope("otlp4j-count-connector", "0.1.0", Attributes.empty(), 0);

    private CountMetrics() {}

    static MetricsData deltaSum(String name, String description, long count) {
        var epochNanos = System.currentTimeMillis() * 1_000_000L;
        var point = new NumberPoint(
                Attributes.empty(), 0L, epochNanos, NumberPoint.longValue(count), 0L);
        var metric = new Metric(
                name,
                description,
                "{item}",
                new Metric.Sum(List.of(point), Metric.AggregationTemporality.DELTA, true),
                Attributes.empty());
        return new MetricsData(List.of(new MetricsData.ResourceMetrics(
                Resource.EMPTY,
                "",
                List.of(new MetricsData.ScopeMetrics(SCOPE, "", List.of(metric))))));
    }

    /// Cross-signal `Partial`/`Rejected` from the derived metric must not be relabelled onto the
    /// input batch; log for visibility and report the input as accepted.
    static <I> ConsumeResult<I> acceptInput(ConsumeResult<MetricsData> downstream, Logger log, String connectorName) {
        switch (downstream) {
            case ConsumeResult.Accepted<MetricsData> _ -> {}
            case ConsumeResult.Partial<MetricsData>(var rejected, var message) ->
                    log.warn("{} downstream partial_success: {} rejected items, msg={}",
                            connectorName, rejected, message);
            case ConsumeResult.Rejected<MetricsData>(var message, var cause) ->
                    log.warn("{} downstream rejected derived metric: {}", connectorName, message, cause);
        }
        return ConsumeResult.accepted();
    }
}
