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
import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.model.TracesData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/// Rich, fully-populated telemetry payloads for the transport tests - one place to build the
/// "every field set" domain objects the integration tests round-trip over the wire.
///
/// The small/basic domain factories live in the shared [Fixtures] testkit; this class keeps
/// only the transport-specific payloads.
public final class TransportFixtures {

    private TransportFixtures() {}

    public static ProfilesData profiles(Profile... profiles) {
        return ProfilesData.of(Resource.EMPTY, InstrumentationScope.EMPTY, List.of(profiles));
    }

    /// A trace with every span field - events, links, status, dropped counts - populated.
    public static TracesData richTraceData() {
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
        return new TracesData(List.of(new TracesData.ResourceSpans(
                new Resource(Attributes.builder().put("service.name", "checkout").build(), 7),
                "https://schema/resource",
                List.of(new TracesData.ScopeSpans(
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

    /// A profile with every field populated: samples of each shape, a link, inlined frames, a mapping,
    /// and attributes with units at every level that takes them.
    public static ProfilesData profilesData() {
        var mapping = Profile.Mapping.builder()
                .memoryStart(0x1000L)
                .memoryLimit(0x9000L)
                .fileOffset(0x100L)
                .filename("/usr/bin/checkout")
                .attributes(Attributes.builder().put("process.executable.build_id.gnu", "abc123").build())
                .build();
        var inlined = new Profile.Function("memcpy", "__memcpy_avx", "memcpy.c", 10L);
        var caller = new Profile.Function("handle", "_ZN6handleEv", "handler.cc", 40L);
        var leaf = Profile.Location.builder()
                .mapping(mapping)
                .address(0x1234L)
                .addLine(new Profile.Line(inlined, 12L, 3L))
                .addLine(new Profile.Line(caller, 44L, 9L))
                .attributes(Attributes.builder().put("profile.frame.type", "native").build())
                .build();
        var root = Profile.Location.builder().mapping(mapping).address(0x2000L).build();
        var stack = new Profile.Stack(List.of(leaf, root));
        var link = new Profile.Link("0102030405060708090a0b0c0d0e0f10", "0102030405060708");
        var heap = Attributes.builder().put("allocation_size", 128L).put("thread.name", "worker").build();
        return profiles(Profile.builder()
                .profileId("0102030405060708090a0b0c0d0e0f10")
                .epochNanos(1_700_000_000_000_000_000L)
                .durationNanos(5_000_000L)
                .sampleType(new Profile.ValueType("cpu", "nanoseconds"))
                .periodType(new Profile.ValueType("cpu", "milliseconds"))
                .period(10L)
                .addSample(Profile.Sample.builder().stack(stack).link(link).addValue(30L).build())
                .addSample(Profile.Sample.builder()
                        .stack(stack)
                        .attributes(heap)
                        .attributeUnits(Map.of("allocation_size", "By"))
                        .addTimestamp(1_700_000_000_000_000_100L)
                        .addTimestamp(1_700_000_000_000_000_200L)
                        .build())
                .addSample(Profile.Sample.builder()
                        .addValue(1L)
                        .addValue(2L)
                        .addTimestamp(1_700_000_000_000_000_300L)
                        .addTimestamp(1_700_000_000_000_000_400L)
                        .build())
                .attributes(Attributes.builder().put("pprof.profile.comment", "nightly").build())
                .droppedAttributesCount(4)
                .originalPayloadFormat("pprof")
                .originalPayload(new byte[] {1, 2, 3})
                .build());
    }

    // --- malformed / mutated inputs for the bug-hunt tests -------------------------------------

    /// A [TracesData] carrying a single span whose traceId is the given (possibly malformed)
    /// hex string - used to probe CommonMapper.bytes input validation through TraceMapper.
    public static TracesData traceWithTraceId(String traceId) {
        return Fixtures.traceData(Span.builder()
                .traceId(traceId)
                .spanId("0102030405060708")
                .name("probe")
                .kind(Span.Kind.INTERNAL)
                .build());
    }

    /// A [TracesData] with a single span whose flags is set to the given value - used to probe
    /// the unsigned-int (int) cast / & 0xFFFFFFFFL decode round-trip.
    public static TracesData traceWithSpanFlags(long flags) {
        return Fixtures.traceData(Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("probe")
                .kind(Span.Kind.INTERNAL)
                .flags(flags)
                .build());
    }

    /// A [TracesData] with one resource holding the given number of single-span scopes - used to
    /// exercise the multi-resource / multi-scope mapper loops.
    public static TracesData traceWithScopes(int scopeCount) {
        var scopes = new ArrayList<TracesData.ScopeSpans>(scopeCount);
        for (var i = 0; i < scopeCount; i++) {
            scopes.add(new TracesData.ScopeSpans(
                    new InstrumentationScope("scope-" + i, "1.0." + i, Attributes.empty(), i),
                    "https://schema/scope/" + i,
                    List.of(Fixtures.span("op-" + i, Span.Kind.INTERNAL))));
        }
        return new TracesData(List.of(
                new TracesData.ResourceSpans(Fixtures.checkoutResource(), "https://schema/r", scopes),
                new TracesData.ResourceSpans(Resource.EMPTY, "", scopes)));
    }
}
