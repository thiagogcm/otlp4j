package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/// A batch of metric telemetry: the domain equivalent of an OTLP
/// `ExportMetricsServiceRequest`.
///
/// Hierarchy: `MetricsData → ResourceMetrics → ScopeMetrics → Metric`.
public record MetricsData(List<ResourceMetrics> resourceMetrics) {

    public MetricsData {
        resourceMetrics = List.copyOf(resourceMetrics);
    }

    /// Wraps `metrics` under one `resource` and `scope`.
    public static MetricsData of(Resource resource, InstrumentationScope scope, List<Metric> metrics) {
        return new MetricsData(
                List.of(new ResourceMetrics(resource, "", List.of(new ScopeMetrics(scope, "", metrics)))));
    }

    /// All metrics across every resource and scope, flattened for convenient
    /// consumption.
    ///
    /// Allocates a fresh list on every call; on a hot path prefer [#forEachMetric].
    public List<Metric> metrics() {
        return resourceMetrics.stream()
                .flatMap(rm -> rm.scopeMetrics().stream())
                .flatMap(sm -> sm.metrics().stream())
                .toList();
    }

    /// Applies `action` to every metric across every resource and scope, in
    /// [#metrics] order without allocating the flattened list.
    public void forEachMetric(Consumer<? super Metric> action) {
        Objects.requireNonNull(action, "action");
        for (var resource : resourceMetrics) {
            for (var scope : resource.scopeMetrics()) {
                for (var metric : scope.metrics()) {
                    action.accept(metric);
                }
            }
        }
    }

    /// The total number of metric data points across every resource, scope,
    /// and metric.
    public long dataPointCount() {
        var count = 0L;
        for (var resource : resourceMetrics) {
            for (var scope : resource.scopeMetrics()) {
                for (var metric : scope.metrics()) {
                    count += switch (metric.data()) {
                        case Metric.NoData _ -> 0L;
                        case Metric.Gauge g -> g.points().size();
                        case Metric.Sum s -> s.points().size();
                        case Metric.Histogram h -> h.points().size();
                        case Metric.ExponentialHistogram e -> e.points().size();
                        case Metric.Summary s -> s.points().size();
                    };
                }
            }
        }
        return count;
    }

    /// Metrics from one [Resource], grouped by instrumentation scope.
    public record ResourceMetrics(
            Resource resource, String schemaUrl, List<ScopeMetrics> scopeMetrics) {
        public ResourceMetrics {
            scopeMetrics = List.copyOf(scopeMetrics);
        }
    }

    /// Metrics produced by a single [InstrumentationScope].
    public record ScopeMetrics(
            InstrumentationScope scope, String schemaUrl, List<Metric> metrics) {
        public ScopeMetrics {
            metrics = List.copyOf(metrics);
        }
    }
}
