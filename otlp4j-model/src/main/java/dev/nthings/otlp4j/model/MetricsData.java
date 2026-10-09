package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

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

    /// Returns this batch with every resource replaced by `mapper`'s result. Schema URLs, scopes, and
    /// metrics are retained. Returns this instance when `mapper` returns each resource unchanged.
    public MetricsData mapResources(UnaryOperator<Resource> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceMetrics(Rewrites.map(resourceMetrics, group -> group.withResource(
                Objects.requireNonNull(mapper.apply(group.resource()), "mapper returned null"))));
    }

    /// Returns this batch with only the resource groups whose resource `keep` accepts. A rejected
    /// group is removed together with its metrics. Returns this instance when `keep` accepts every
    /// resource.
    public MetricsData filterResources(Predicate<? super Resource> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceMetrics(Rewrites.filter(resourceMetrics, group -> keep.test(group.resource())));
    }

    /// Returns this batch with every metric replaced by `mapper`'s result, which must be non-null.
    /// Resource and scope grouping is retained. To select or change data points, return a metric with
    /// replaced [Metric#data()]. Returns this instance when `mapper` returns each metric unchanged.
    public MetricsData mapMetrics(UnaryOperator<Metric> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceMetrics(Rewrites.map(resourceMetrics, resource -> resource.withScopeMetrics(
                Rewrites.map(resource.scopeMetrics(), scope -> scope.withMetrics(
                        Rewrites.map(scope.metrics(), mapper))))));
    }

    /// Returns this batch with only the metrics `keep` accepts, in their original order and
    /// grouping. A scope or resource that loses all its metrics is removed; groups that lose nothing
    /// are kept as they are. A kept metric retains all its data points. Returns this instance when
    /// `keep` accepts every metric.
    public MetricsData filterMetrics(Predicate<? super Metric> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceMetrics(Rewrites.prune(
                resourceMetrics,
                resource -> resource.withScopeMetrics(Rewrites.prune(
                        resource.scopeMetrics(),
                        scope -> scope.withMetrics(Rewrites.filter(scope.metrics(), keep)),
                        scope -> scope.metrics().isEmpty())),
                resource -> resource.scopeMetrics().isEmpty()));
    }

    private MetricsData withResourceMetrics(List<ResourceMetrics> replacement) {
        return replacement == resourceMetrics ? this : new MetricsData(replacement);
    }

    /// Metrics from one [Resource], grouped by instrumentation scope.
    public record ResourceMetrics(
            Resource resource, String schemaUrl, List<ScopeMetrics> scopeMetrics) {
        public ResourceMetrics {
            scopeMetrics = List.copyOf(scopeMetrics);
        }

        ResourceMetrics withResource(Resource replacement) {
            return replacement == resource ? this : new ResourceMetrics(replacement, schemaUrl, scopeMetrics);
        }

        ResourceMetrics withScopeMetrics(List<ScopeMetrics> replacement) {
            return replacement == scopeMetrics ? this : new ResourceMetrics(resource, schemaUrl, replacement);
        }
    }

    /// Metrics produced by a single [InstrumentationScope].
    public record ScopeMetrics(
            InstrumentationScope scope, String schemaUrl, List<Metric> metrics) {
        public ScopeMetrics {
            metrics = List.copyOf(metrics);
        }

        ScopeMetrics withMetrics(List<Metric> replacement) {
            return replacement == metrics ? this : new ScopeMetrics(scope, schemaUrl, replacement);
        }
    }
}
