package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/// A batch of metric telemetry: the domain equivalent of an OTLP `ExportMetricsServiceRequest`.
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

    /// All metrics across every resource and scope, flattened for convenient consumption.
    ///
    /// This walks the resource/scope grouping and allocates a fresh list on every call. On a hot
    /// path prefer [#forEachMetric] to visit metrics without the intermediate list, or
    /// [#metricCount] to size the batch without flattening it.
    public List<Metric> metrics() {
        return resourceMetrics.stream()
                .flatMap(rm -> rm.scopeMetrics().stream())
                .flatMap(sm -> sm.metrics().stream())
                .toList();
    }

    /// Applies `action` to every metric across every resource and scope, in the same order as
    /// [#metrics], without allocating the intermediate flattened list.
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

    /// The total number of [Metric] objects across every resource and scope, counted without
    /// allocating the flattened list that [#metrics] builds. Note this counts metrics, not the
    /// nested data points OTLP reports for metric partial-success.
    public int metricCount() {
        var count = 0;
        for (var resource : resourceMetrics) {
            for (var scope : resource.scopeMetrics()) {
                count += scope.metrics().size();
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
