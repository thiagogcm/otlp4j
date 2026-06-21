package dev.nthings.otlp4j.model;

import java.util.List;

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
    public List<Metric> metrics() {
        return resourceMetrics.stream()
                .flatMap(rm -> rm.scopeMetrics().stream())
                .flatMap(sm -> sm.metrics().stream())
                .toList();
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
