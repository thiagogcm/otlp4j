package dev.nthings.otlp4j.testing;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.HistogramPoint;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.model.TraceData;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/// Rich, fully-populated telemetry payloads for the transport tests — one place to build the
/// "every field set" domain objects the integration tests round-trip over the wire.
///
/// The small/basic domain factories live in the shared [Fixtures] testkit; this class keeps
/// only the transport-specific payloads.
public final class TransportFixtures {

    private TransportFixtures() {}

    public static ProfilesData profiles(ProfilesData.Profile... profiles) {
        return new ProfilesData(List.of(new ProfilesData.ResourceProfiles(
                Resource.EMPTY,
                "",
                List.of(new ProfilesData.ScopeProfiles(
                        InstrumentationScope.EMPTY, "", List.of(profiles))))), new byte[0]);
    }

    /// A trace with every span field — events, links, status, dropped counts — populated.
    public static TraceData richTraceData() {
        var span = Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .parentSpanId("1112131415161718")
                .traceState("vendor=abc")
                .flags(1L)
                .name("GET /cart")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                .attributes(Attributes.builder()
                        .put("http.method", "GET")
                        .put("http.status", 200L)
                        .build())
                .droppedAttributesCount(3)
                .addEvent(new Span.Event(
                        1_500L, "cache.miss", Attributes.builder().put("key", "cart:42").build(), 1))
                .droppedEventsCount(4)
                .addLink(new Span.Link(
                        "aabbccddeeff00112233445566778899",
                        "aabbccddeeff0011",
                        "vendor=xyz",
                        Attributes.builder().put("link.kind", "follows-from").build(),
                        5,
                        2L))
                .droppedLinksCount(6)
                .status(new Span.Status(Span.Status.Code.ERROR, "boom"))
                .build();
        return new TraceData(List.of(new TraceData.ResourceSpans(
                new Resource(Attributes.builder().put("service.name", "checkout").build(), 7),
                "https://schema/resource",
                List.of(new TraceData.ScopeSpans(
                        new InstrumentationScope(
                                "otlp4j-test",
                                "1.0.0",
                                Attributes.builder().put("scope.attr", true).build(),
                                8),
                        "https://schema/scope",
                        List.of(span))))));
    }

    /// Metrics covering all five data kinds: gauge, sum, histogram, exponential histogram, summary.
    public static MetricsData richMetricsData() {
        var gauge = Metric.builder()
                .name("process.cpu.utilization")
                .description("CPU usage")
                .unit("1")
                .data(new Metric.Gauge(List.of(new NumberPoint(
                        Attributes.builder().put("core", 0L).build(),
                        1_000L,
                        2_000L,
                        NumberPoint.doubleValue(0.73),
                        0L,
                        List.of()))))
                .build();
        var sum = Metric.builder()
                .name("http.server.requests")
                .description("Request count")
                .unit("{request}")
                .data(new Metric.Sum(
                        List.of(new NumberPoint(
                                Attributes.empty(),
                                1_000L,
                                2_000L,
                                NumberPoint.longValue(125L),
                                0L,
                                List.of())),
                        Metric.AggregationTemporality.CUMULATIVE,
                        true))
                .metadata(Attributes.builder().put("meta", "x").build())
                .build();
        var histogram = Metric.builder()
                .name("http.request.duration")
                .unit("ms")
                .data(new Metric.Histogram(
                        List.of(new HistogramPoint(
                                Attributes.builder().put("route", "/cart").build(),
                                1_000L,
                                2_000L,
                                10L,
                                OptionalDouble.of(123.4),
                                List.of(2L, 3L, 5L),
                                List.of(1.0, 10.0),
                                OptionalDouble.of(0.5),
                                OptionalDouble.of(99.9),
                                0L,
                                List.of())),
                        Metric.AggregationTemporality.CUMULATIVE))
                .build();
        var exponential = Metric.builder()
                .name("db.query.duration")
                .data(new Metric.ExponentialHistogram(
                        List.of(new ExponentialHistogramPoint(
                                Attributes.empty(),
                                1_000L,
                                2_000L,
                                7L,
                                OptionalDouble.of(50.0),
                                2,
                                1L,
                                new ExponentialHistogramPoint.Buckets(0, List.of(1L, 2L, 1L)),
                                ExponentialHistogramPoint.Buckets.EMPTY,
                                OptionalDouble.empty(),
                                OptionalDouble.empty(),
                                0.0,
                                0L,
                                List.of())),
                        Metric.AggregationTemporality.DELTA))
                .build();
        var summary = Metric.builder()
                .name("rpc.duration")
                .data(new Metric.Summary(List.of(new SummaryPoint(
                        Attributes.empty(),
                        1_000L,
                        2_000L,
                        100L,
                        543.2,
                        List.of(
                                new SummaryPoint.Quantile(0.5, 10.0),
                                new SummaryPoint.Quantile(0.99, 99.0)),
                        0L))))
                .build();
        return Fixtures.metricsData(gauge, sum, histogram, exponential, summary);
    }

    /// A log batch with every log-record field populated.
    public static LogsData richLogsData() {
        var record = LogRecord.builder()
                .epochNanos(1_700_000_000_000_000_000L)
                .observedEpochNanos(1_700_000_000_000_000_001L)
                .severity(LogRecord.Severity.ERROR)
                .severityText("ERROR")
                .body("payment gateway timeout")
                .attributes(Attributes.builder().put("error.code", 504L).build())
                .droppedAttributesCount(2)
                .flags(1L)
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .eventName("payment.failed")
                .build();
        return Fixtures.logsData(record);
    }

    /// A profiles batch built from scalar metadata only (empty `rawProfile`), so it re-emits just
    /// the modeled scalar fields. Lossless sample/dictionary forwarding is covered by the dedicated
    /// round-trip test that drives the proto builders directly.
    public static ProfilesData profilesData() {
        return profiles(new ProfilesData.Profile(
                "0102030405060708090a0b0c0d0e0f10",
                1_700_000_000_000_000_000L,
                5_000_000L,
                99L,
                0,
                4,
                "pprof",
                new byte[0]));
    }

    // --- malformed / mutated inputs for the bug-hunt tests -------------------------------------

    /// A [TraceData] carrying a single span whose `traceId` is the given (possibly malformed)
    /// hex string — used to probe `CommonMapper.bytes` input validation through `TraceMapper`.
    public static TraceData traceWithTraceId(String traceId) {
        return Fixtures.traceData(Span.builder()
                .traceId(traceId)
                .spanId("0102030405060708")
                .name("probe")
                .kind(Span.Kind.INTERNAL)
                .build());
    }

    /// A [TraceData] with a single span whose `flags` is set to the given value — used to probe
    /// the unsigned-int `(int)` cast / `& 0xFFFFFFFFL` decode round-trip.
    public static TraceData traceWithSpanFlags(long flags) {
        return Fixtures.traceData(Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("probe")
                .kind(Span.Kind.INTERNAL)
                .flags(flags)
                .build());
    }

    /// A [TraceData] with one resource holding the given number of single-span scopes — used to
    /// exercise the multi-resource / multi-scope mapper loops.
    public static TraceData traceWithScopes(int scopeCount) {
        var scopes = new ArrayList<TraceData.ScopeSpans>(scopeCount);
        for (var i = 0; i < scopeCount; i++) {
            scopes.add(new TraceData.ScopeSpans(
                    new InstrumentationScope("scope-" + i, "1.0." + i, Attributes.empty(), i),
                    "https://schema/scope/" + i,
                    List.of(Fixtures.span("op-" + i, Span.Kind.INTERNAL))));
        }
        return new TraceData(List.of(
                new TraceData.ResourceSpans(Fixtures.checkoutResource(), "https://schema/r", scopes),
                new TraceData.ResourceSpans(Resource.EMPTY, "", scopes)));
    }
}
