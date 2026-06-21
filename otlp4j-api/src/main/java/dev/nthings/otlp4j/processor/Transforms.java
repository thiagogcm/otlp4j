package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.Transform;
import java.util.ArrayList;
import java.util.function.Predicate;

/// Ready-made [Transform]s for the common pipeline operations.
///
/// Each helper is typed to the OTLP signal it operates on; plug into
/// `Pipeline.from(...).transform(...)`.
public final class Transforms {

    private Transforms() {}

    /// Filters spans by `keep`; scopes and resources with no surviving spans are pruned.
    public static Transform<TraceData> keepSpansWhere(Predicate<Span> keep) {
        return traces -> {
            var resources = new ArrayList<TraceData.ResourceSpans>();
            for (var rs : traces.resourceSpans()) {
                var scopes = new ArrayList<TraceData.ScopeSpans>();
                for (var ss : rs.scopeSpans()) {
                    var kept = ss.spans().stream().filter(keep).toList();
                    if (!kept.isEmpty()) {
                        scopes.add(new TraceData.ScopeSpans(ss.scope(), ss.schemaUrl(), kept));
                    }
                }
                if (!scopes.isEmpty()) {
                    resources.add(new TraceData.ResourceSpans(rs.resource(), rs.schemaUrl(), scopes));
                }
            }
            return new TraceData(resources);
        };
    }

    /// Filters log records by `keep`; scopes and resources with no surviving records are pruned.
    public static Transform<LogsData> keepLogRecordsWhere(Predicate<LogRecord> keep) {
        return logs -> {
            var resources = new ArrayList<LogsData.ResourceLogs>();
            for (var rl : logs.resourceLogs()) {
                var scopes = new ArrayList<LogsData.ScopeLogs>();
                for (var sl : rl.scopeLogs()) {
                    var kept = sl.logRecords().stream().filter(keep).toList();
                    if (!kept.isEmpty()) {
                        scopes.add(new LogsData.ScopeLogs(sl.scope(), sl.schemaUrl(), kept));
                    }
                }
                if (!scopes.isEmpty()) {
                    resources.add(new LogsData.ResourceLogs(rl.resource(), rl.schemaUrl(), scopes));
                }
            }
            return new LogsData(resources);
        };
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a trace batch.
    public static Transform<TraceData> withTracesResourceAttribute(String key, AttributeValue value) {
        return traces -> new TraceData(traces.resourceSpans().stream()
                .map(rs -> new TraceData.ResourceSpans(enrich(rs.resource(), key, value), rs.schemaUrl(), rs.scopeSpans()))
                .toList());
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a metrics batch.
    public static Transform<MetricsData> withMetricsResourceAttribute(String key, AttributeValue value) {
        return metrics -> new MetricsData(metrics.resourceMetrics().stream()
                .map(rm -> new MetricsData.ResourceMetrics(enrich(rm.resource(), key, value), rm.schemaUrl(), rm.scopeMetrics()))
                .toList());
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a logs batch.
    public static Transform<LogsData> withLogsResourceAttribute(String key, AttributeValue value) {
        return logs -> new LogsData(logs.resourceLogs().stream()
                .map(rl -> new LogsData.ResourceLogs(enrich(rl.resource(), key, value), rl.schemaUrl(), rl.scopeLogs()))
                .toList());
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a profiles batch.
    public static Transform<ProfilesData> withProfilesResourceAttribute(String key, AttributeValue value) {
        return profiles -> new ProfilesData(profiles.resourceProfiles().stream()
                .map(rp -> new ProfilesData.ResourceProfiles(enrich(rp.resource(), key, value), rp.schemaUrl(), rp.scopeProfiles()))
                .toList());
    }

    private static Resource enrich(Resource resource, String key, AttributeValue value) {
        return new Resource(
                resource.attributes().toBuilder().put(key, value).build(),
                resource.droppedAttributesCount());
    }
}
