package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.ForwardingConsumer;
import dev.nthings.otlp4j.pipeline.Processor;
import java.util.ArrayList;
import java.util.function.Predicate;

/// Ready-made [Processor]s for the common pipeline transforms.
///
/// Each returns a stateless, composable `Processor`; chain them with
/// [Processor#andThen] or stack them in a `Pipeline`. Signals a processor does not touch
/// flow downstream unchanged.
public final class Processors {

    private Processors() {}

    /// Keeps only spans matching `keep`. Scopes and resources left with no spans are pruned.
    /// Metrics, logs and profiles pass through untouched.
    public static Processor filterSpans(Predicate<Span> keep) {
        return next -> new ForwardingConsumer(next) {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
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
                        resources.add(
                                new TraceData.ResourceSpans(rs.resource(), rs.schemaUrl(), scopes));
                    }
                }
                return delegate().consumeTraces(new TraceData(resources));
            }
        };
    }

    /// Keeps only log records matching `keep`. Scopes and resources left with no records are
    /// pruned. Traces, metrics and profiles pass through untouched.
    public static Processor filterLogRecords(Predicate<LogRecord> keep) {
        return next -> new ForwardingConsumer(next) {
            @Override
            public ExportResult consumeLogs(LogsData logs) {
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
                        resources.add(
                                new LogsData.ResourceLogs(rl.resource(), rl.schemaUrl(), scopes));
                    }
                }
                return delegate().consumeLogs(new LogsData(resources));
            }
        };
    }

    /// Sets (adds or overwrites) a resource attribute on every signal — traces, metrics, logs and
    /// profiles alike. Useful for stamping telemetry with deployment-wide context.
    public static Processor setResourceAttribute(String key, AttributeValue value) {
        return next -> new ForwardingConsumer(next) {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                return delegate().consumeTraces(new TraceData(traces.resourceSpans().stream()
                        .map(rs -> new TraceData.ResourceSpans(
                                enrich(rs.resource(), key, value),
                                rs.schemaUrl(),
                                rs.scopeSpans()))
                        .toList()));
            }

            @Override
            public ExportResult consumeMetrics(MetricsData metrics) {
                return delegate().consumeMetrics(new MetricsData(metrics.resourceMetrics().stream()
                        .map(rm -> new MetricsData.ResourceMetrics(
                                enrich(rm.resource(), key, value),
                                rm.schemaUrl(),
                                rm.scopeMetrics()))
                        .toList()));
            }

            @Override
            public ExportResult consumeLogs(LogsData logs) {
                return delegate().consumeLogs(new LogsData(logs.resourceLogs().stream()
                        .map(rl -> new LogsData.ResourceLogs(
                                enrich(rl.resource(), key, value),
                                rl.schemaUrl(),
                                rl.scopeLogs()))
                        .toList()));
            }

            @Override
            public ExportResult consumeProfiles(ProfilesData profiles) {
                return delegate().consumeProfiles(new ProfilesData(profiles.resourceProfiles().stream()
                        .map(rp -> new ProfilesData.ResourceProfiles(
                                enrich(rp.resource(), key, value),
                                rp.schemaUrl(),
                                rp.scopeProfiles()))
                        .toList()));
            }
        };
    }

    private static Resource enrich(Resource resource, String key, AttributeValue value) {
        var builder = Attributes.builder();
        resource.attributes().asMap().forEach(builder::put);
        builder.put(key, value);
        return new Resource(builder.build(), resource.droppedAttributesCount());
    }
}
