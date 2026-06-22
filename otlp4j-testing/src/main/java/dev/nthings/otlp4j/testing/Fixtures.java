package dev.nthings.otlp4j.testing;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import java.util.List;

/// Shared test-data factories for otlp4j test suites.
///
/// Keep small reusable domain objects here; module-specific rich fixtures belong with that module's
/// own tests.
public final class Fixtures {

    private Fixtures() {}

    public static Span span(String name, Span.Kind kind) {
        return Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name(name)
                .kind(kind)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                .build();
    }

    public static TraceData traceData(Span... spans) {
        return TraceData.of(checkoutResource(), testScope(), List.of(spans));
    }

    public static MetricsData metricsData(Metric... metrics) {
        return MetricsData.of(checkoutResource(), testScope(), List.of(metrics));
    }

    public static Metric metric(String name) {
        var point = new NumberPoint(
                Attributes.empty(), 0L, 2_000L, NumberPoint.longValue(1L), 0L, List.of());
        return Metric.builder()
                .name(name)
                .data(new Metric.Gauge(List.of(point)))
                .build();
    }

    public static ProfilesData profilesData(ProfilesData.Profile... profiles) {
        return ProfilesData.of(checkoutResource(), testScope(), List.of(profiles));
    }

    public static ProfilesData.Profile profile(String profileId) {
        return new ProfilesData.Profile(profileId, 1_000L, 500L, 0L, 0, 0, "", new byte[0]);
    }

    public static LogRecord logRecord(String body, LogRecord.Severity severity) {
        return LogRecord.builder()
                .epochNanos(1_700_000_000_000_000_000L)
                .severity(severity)
                .body(body)
                .build();
    }

    public static LogsData logsData(LogRecord... records) {
        return LogsData.of(checkoutResource(), testScope(), List.of(records));
    }

    public static Resource checkoutResource() {
        return Resource.of(Attributes.builder().put("service.name", "checkout").build());
    }

    public static InstrumentationScope testScope() {
        return InstrumentationScope.of("otlp4j-test", "1.0.0");
    }
}
