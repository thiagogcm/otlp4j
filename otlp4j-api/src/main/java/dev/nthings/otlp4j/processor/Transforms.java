package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Transform;
import java.util.ArrayList;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Ready-made [Transform]s for the common pipeline operations.
///
/// Each helper is typed to the OTLP signal it operates on; plug into
/// `Pipeline.from(...).transform(...)`.
public final class Transforms {

    private Transforms() {}

    /// Filters spans by `keep`; scopes and resources with no surviving spans are pruned.
    public static Transform<TracesData> keepSpansWhere(Predicate<Span> keep) {
        return traces -> {
            var resources = new ArrayList<TracesData.ResourceSpans>();
            for (var rs : traces.resourceSpans()) {
                var scopes = new ArrayList<TracesData.ScopeSpans>();
                for (var ss : rs.scopeSpans()) {
                    var kept = ss.spans().stream().filter(keep).toList();
                    if (!kept.isEmpty()) {
                        scopes.add(new TracesData.ScopeSpans(ss.scope(), ss.schemaUrl(), kept));
                    }
                }
                if (!scopes.isEmpty()) {
                    resources.add(new TracesData.ResourceSpans(rs.resource(), rs.schemaUrl(), scopes));
                }
            }
            return new TracesData(resources);
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

    /// Rewrites every span 1:1 with `mapper`; scopes and resources are kept, never pruned (use
    /// [#keepSpansWhere] to drop).
    public static Transform<TracesData> mapSpans(UnaryOperator<Span> mapper) {
        return traces -> new TracesData(traces.resourceSpans().stream()
                .map(rs -> new TracesData.ResourceSpans(
                        rs.resource(),
                        rs.schemaUrl(),
                        rs.scopeSpans().stream()
                                .map(ss -> new TracesData.ScopeSpans(
                                        ss.scope(),
                                        ss.schemaUrl(),
                                        ss.spans().stream().map(mapper).toList()))
                                .toList()))
                .toList());
    }

    /// Rewrites every log record 1:1 with `mapper`; scopes and resources are kept, never pruned (use
    /// [#keepLogRecordsWhere] to drop).
    public static Transform<LogsData> mapLogRecords(UnaryOperator<LogRecord> mapper) {
        return logs -> new LogsData(logs.resourceLogs().stream()
                .map(rl -> new LogsData.ResourceLogs(
                        rl.resource(),
                        rl.schemaUrl(),
                        rl.scopeLogs().stream()
                                .map(sl -> new LogsData.ScopeLogs(
                                        sl.scope(),
                                        sl.schemaUrl(),
                                        sl.logRecords().stream().map(mapper).toList()))
                                .toList()))
                .toList());
    }

    /// Rewrites every [Resource] in a trace batch with `mapper`.
    public static Transform<TracesData> mapTracesResource(UnaryOperator<Resource> mapper) {
        return traces -> new TracesData(traces.resourceSpans().stream()
                .map(rs -> new TracesData.ResourceSpans(
                        mapper.apply(rs.resource()), rs.schemaUrl(), rs.scopeSpans()))
                .toList());
    }

    /// Rewrites every [Resource] in a metrics batch with `mapper`.
    public static Transform<MetricsData> mapMetricsResource(UnaryOperator<Resource> mapper) {
        return metrics -> new MetricsData(metrics.resourceMetrics().stream()
                .map(rm -> new MetricsData.ResourceMetrics(
                        mapper.apply(rm.resource()), rm.schemaUrl(), rm.scopeMetrics()))
                .toList());
    }

    /// Rewrites every [Resource] in a logs batch with `mapper`.
    public static Transform<LogsData> mapLogsResource(UnaryOperator<Resource> mapper) {
        return logs -> new LogsData(logs.resourceLogs().stream()
                .map(rl -> new LogsData.ResourceLogs(
                        mapper.apply(rl.resource()), rl.schemaUrl(), rl.scopeLogs()))
                .toList());
    }

    /// Rewrites every [Resource] in a profiles batch with `mapper`; the shared dictionary is kept.
    public static Transform<ProfilesData> mapProfilesResource(UnaryOperator<Resource> mapper) {
        return profiles -> new ProfilesData(
                profiles.resourceProfiles().stream()
                        .map(rp -> new ProfilesData.ResourceProfiles(
                                mapper.apply(rp.resource()), rp.schemaUrl(), rp.scopeProfiles()))
                        .toList(),
                profiles.dictionary());
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a trace batch.
    public static Transform<TracesData> withTracesResourceAttribute(String key, AttributeValue value) {
        return mapTracesResource(resource -> resource.withAttribute(key, value));
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a metrics batch.
    public static Transform<MetricsData> withMetricsResourceAttribute(String key, AttributeValue value) {
        return mapMetricsResource(resource -> resource.withAttribute(key, value));
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a logs batch.
    public static Transform<LogsData> withLogsResourceAttribute(String key, AttributeValue value) {
        return mapLogsResource(resource -> resource.withAttribute(key, value));
    }

    /// Adds (or overwrites) a resource attribute on every [Resource] in a profiles batch.
    public static Transform<ProfilesData> withProfilesResourceAttribute(String key, AttributeValue value) {
        return mapProfilesResource(resource -> resource.withAttribute(key, value));
    }
}
