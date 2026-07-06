package dev.nthings.otlp4j.codec.property;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Exemplar;
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
import dev.nthings.otlp4j.model.TracesData;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.profiles.v1development.KeyValueAndUnit;
import io.opentelemetry.proto.profiles.v1development.Profile;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import io.opentelemetry.proto.profiles.v1development.ValueType;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span.Event;
import io.opentelemetry.proto.trace.v1.Span.Link;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

/// Seeded randomized generators for the codec property tests.
///
/// Every generator accepts a [RandomGenerator] so a caller can produce a reproducible sequence of
/// samples from a single seed; regenerating from the same seed regenerates the same value. Each
/// signal has a domain-side generator (for the domain to proto to domain invariant) and a
/// proto-side generator that builds the wire message directly with the generated proto builders,
/// never routing through a mapper (for the proto to domain to proto invariant). Both generators are
/// deliberately restricted to the field surface the domain model represents, exactly as the design
/// requires: an id field is either empty or exactly the wire-valid length, an enum field only takes
/// a number the domain enum also declares, and a bucket-count list is only ever shaped the way the
/// domain's own construction-time invariant allows. Restricting generation this way is what makes
/// the round trip assertions equalities rather than best-effort comparisons.
final class ModelGen {

    private ModelGen() {}

    /// One generated sample paired with the seed that produced it. The seed is carried into the
    /// assertion description of every property test so a failing case is reproducible by calling
    /// [#seeded(long)] with the same value again.
    record Case<T>(long seed, T value) {
        @Override
        public String toString() {
            return "seed=" + seed;
        }
    }

    private static final int MAX_ATTRIBUTE_DEPTH = 2;
    private static final long UINT32_EXCLUSIVE_BOUND = 0x1_0000_0000L;
    private static final int TRACE_ID_HEX_LEN = 32;
    private static final int SPAN_ID_HEX_LEN = 16;
    private static final int TRACE_ID_BYTE_LEN = 16;
    private static final int SPAN_ID_BYTE_LEN = 8;
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private static final String PRINTABLE_ASCII =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .-_/:";

    private static final Span.Kind[] SPAN_KINDS = Span.Kind.values();
    private static final Span.Status.Code[] STATUS_CODES = Span.Status.Code.values();
    private static final int[] SPAN_KIND_NUMBERS = {0, 1, 2, 3, 4, 5};
    private static final int[] STATUS_CODE_NUMBERS = {0, 1, 2};
    private static final Metric.AggregationTemporality[] TEMPORALITIES =
            Metric.AggregationTemporality.values();
    private static final int[] TEMPORALITY_NUMBERS = {0, 1, 2};
    private static final LogRecord.Severity[] SEVERITIES = LogRecord.Severity.values();
    private static final int[] SEVERITY_NUMBERS = buildSeverityNumbers();

    private static int[] buildSeverityNumbers() {
        var numbers = new int[SEVERITIES.length];
        for (var i = 0; i < numbers.length; i++) {
            numbers[i] = SEVERITIES[i].number();
        }
        return numbers;
    }

    /// A new deterministic generator seeded with `seed`. The same seed always produces the same
    /// sequence of generated values, which is what lets a failing property test's seed be handed
    /// back here to reproduce the counterexample.
    static RandomGenerator seeded(long seed) {
        return RandomGeneratorFactory.of("L64X128MixRandom").create(seed);
    }

    // ---- shared primitives ----

    private static String hexId(RandomGenerator rng, int hexLength) {
        if (rng.nextInt(4) == 0) {
            return ""; // an absent id, exactly as valid as a present one
        }
        var chars = new char[hexLength];
        for (var i = 0; i < hexLength; i++) {
            chars[i] = HEX_CHARS[rng.nextInt(16)];
        }
        return new String(chars);
    }

    private static ByteString protoId(RandomGenerator rng, int byteLength) {
        if (rng.nextInt(4) == 0) {
            return ByteString.EMPTY;
        }
        return ByteString.copyFrom(randomBytes(rng, byteLength));
    }

    private static byte[] randomBytes(RandomGenerator rng, int length) {
        var bytes = new byte[length];
        rng.nextBytes(bytes);
        return bytes;
    }

    private static String asciiString(RandomGenerator rng, int maxLen) {
        var len = rng.nextInt(maxLen + 1);
        var chars = new char[len];
        for (var i = 0; i < len; i++) {
            chars[i] = PRINTABLE_ASCII.charAt(rng.nextInt(PRINTABLE_ASCII.length()));
        }
        return new String(chars);
    }

    private static double boundedDouble(RandomGenerator rng) {
        return rng.nextDouble(-1_000_000.0, 1_000_000.0);
    }

    private static long uint32(RandomGenerator rng) {
        return rng.nextLong(0L, UINT32_EXCLUSIVE_BOUND);
    }

    private static long positiveLong(RandomGenerator rng) {
        return rng.nextLong(0L, Long.MAX_VALUE);
    }

    // ---- attributes, shared by every signal ----

    static Attributes attributes(RandomGenerator rng) {
        return attributes(rng, 0);
    }

    private static Attributes attributes(RandomGenerator rng, int depth) {
        var n = rng.nextInt(4);
        if (n == 0) {
            return Attributes.empty();
        }
        var builder = Attributes.builder();
        for (var i = 0; i < n; i++) {
            builder.put("attr" + i + "-" + rng.nextInt(1_000), attributeValue(rng, depth));
        }
        return builder.build();
    }

    private static AttributeValue attributeValue(RandomGenerator rng, int depth) {
        var kind = depth >= MAX_ATTRIBUTE_DEPTH ? rng.nextInt(6) : rng.nextInt(8);
        return switch (kind) {
            case 0 -> AttributeValue.of(asciiString(rng, 12));
            case 1 -> AttributeValue.of(rng.nextBoolean());
            case 2 -> AttributeValue.of(rng.nextLong());
            case 3 -> AttributeValue.of(boundedDouble(rng));
            case 4 -> AttributeValue.of(randomBytes(rng, rng.nextInt(9)));
            case 5 -> AttributeValue.empty();
            case 6 -> {
                var size = rng.nextInt(3);
                var values = new ArrayList<AttributeValue>(size);
                for (var i = 0; i < size; i++) {
                    values.add(attributeValue(rng, depth + 1));
                }
                yield AttributeValue.of(values);
            }
            default -> {
                var size = rng.nextInt(3);
                var map = new LinkedHashMap<String, AttributeValue>();
                for (var i = 0; i < size; i++) {
                    map.put("nested" + i, attributeValue(rng, depth + 1));
                }
                yield AttributeValue.of(map);
            }
        };
    }

    private static List<KeyValue> protoKeyValues(RandomGenerator rng) {
        return protoKeyValues(rng, 0);
    }

    private static List<KeyValue> protoKeyValues(RandomGenerator rng, int depth) {
        var n = rng.nextInt(4);
        var result = new ArrayList<KeyValue>(n);
        for (var i = 0; i < n; i++) {
            result.add(KeyValue.newBuilder()
                    .setKey("attr" + i + "-" + rng.nextInt(1_000))
                    .setValue(protoAnyValue(rng, depth))
                    .build());
        }
        return result;
    }

    private static AnyValue protoAnyValue(RandomGenerator rng, int depth) {
        var kind = depth >= MAX_ATTRIBUTE_DEPTH ? rng.nextInt(6) : rng.nextInt(8);
        return switch (kind) {
            case 0 -> AnyValue.newBuilder().setStringValue(asciiString(rng, 12)).build();
            case 1 -> AnyValue.newBuilder().setBoolValue(rng.nextBoolean()).build();
            case 2 -> AnyValue.newBuilder().setIntValue(rng.nextLong()).build();
            case 3 -> AnyValue.newBuilder().setDoubleValue(boundedDouble(rng)).build();
            case 4 ->
                AnyValue.newBuilder()
                        .setBytesValue(ByteString.copyFrom(randomBytes(rng, rng.nextInt(9))))
                        .build();
            case 5 -> AnyValue.getDefaultInstance();
            case 6 -> {
                var size = rng.nextInt(3);
                var array = ArrayValue.newBuilder();
                for (var i = 0; i < size; i++) {
                    array.addValues(protoAnyValue(rng, depth + 1));
                }
                yield AnyValue.newBuilder().setArrayValue(array).build();
            }
            default -> {
                var size = rng.nextInt(3);
                var list = KeyValueList.newBuilder();
                for (var i = 0; i < size; i++) {
                    list.addValues(KeyValue.newBuilder()
                            .setKey("nested" + i)
                            .setValue(protoAnyValue(rng, depth + 1)));
                }
                yield AnyValue.newBuilder().setKvlistValue(list).build();
            }
        };
    }

    // ---- resource / instrumentation scope, shared by every signal ----

    private static Resource resource(RandomGenerator rng) {
        return new Resource(attributes(rng), rng.nextInt(50));
    }

    private static InstrumentationScope scope(RandomGenerator rng) {
        return new InstrumentationScope(
                asciiString(rng, 10), asciiString(rng, 6), attributes(rng), rng.nextInt(50));
    }

    private static io.opentelemetry.proto.resource.v1.Resource protoResource(RandomGenerator rng) {
        return io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                .addAllAttributes(protoKeyValues(rng))
                .setDroppedAttributesCount(rng.nextInt(50))
                .build();
    }

    private static io.opentelemetry.proto.common.v1.InstrumentationScope protoScope(
            RandomGenerator rng) {
        return io.opentelemetry.proto.common.v1.InstrumentationScope.newBuilder()
                .setName(asciiString(rng, 10))
                .setVersion(asciiString(rng, 6))
                .addAllAttributes(protoKeyValues(rng))
                .setDroppedAttributesCount(rng.nextInt(50))
                .build();
    }

    // ---- traces ----

    private static Span span(RandomGenerator rng) {
        var builder = Span.builder()
                .traceId(hexId(rng, TRACE_ID_HEX_LEN))
                .spanId(hexId(rng, SPAN_ID_HEX_LEN))
                .parentSpanId(hexId(rng, SPAN_ID_HEX_LEN))
                .traceState(asciiString(rng, 10))
                .flags(uint32(rng))
                .name(asciiString(rng, 16))
                .kind(SPAN_KINDS[rng.nextInt(SPAN_KINDS.length)])
                .startEpochNanos(positiveLong(rng))
                .endEpochNanos(positiveLong(rng))
                .attributes(attributes(rng))
                .droppedAttributesCount(rng.nextInt(20))
                .droppedEventsCount(rng.nextInt(20))
                .droppedLinksCount(rng.nextInt(20))
                .status(new Span.Status(
                        STATUS_CODES[rng.nextInt(STATUS_CODES.length)], asciiString(rng, 20)));
        var eventCount = rng.nextInt(3);
        for (var i = 0; i < eventCount; i++) {
            builder.addEvent(new Span.Event(
                    positiveLong(rng), asciiString(rng, 8), attributes(rng), rng.nextInt(10)));
        }
        var linkCount = rng.nextInt(3);
        for (var i = 0; i < linkCount; i++) {
            builder.addLink(new Span.Link(
                    hexId(rng, TRACE_ID_HEX_LEN),
                    hexId(rng, SPAN_ID_HEX_LEN),
                    asciiString(rng, 10),
                    attributes(rng),
                    rng.nextInt(10),
                    uint32(rng)));
        }
        return builder.build();
    }

    static TracesData tracesData(RandomGenerator rng) {
        var resourceCount = 1 + rng.nextInt(2);
        var resourceSpansList = new ArrayList<TracesData.ResourceSpans>(resourceCount);
        for (var r = 0; r < resourceCount; r++) {
            var scopeCount = 1 + rng.nextInt(2);
            var scopeSpansList = new ArrayList<TracesData.ScopeSpans>(scopeCount);
            for (var s = 0; s < scopeCount; s++) {
                var spanCount = 1 + rng.nextInt(3);
                var spans = new ArrayList<Span>(spanCount);
                for (var i = 0; i < spanCount; i++) {
                    spans.add(span(rng));
                }
                scopeSpansList.add(new TracesData.ScopeSpans(scope(rng), asciiString(rng, 12), spans));
            }
            resourceSpansList.add(
                    new TracesData.ResourceSpans(resource(rng), asciiString(rng, 12), scopeSpansList));
        }
        return new TracesData(resourceSpansList);
    }

    private static io.opentelemetry.proto.trace.v1.Span protoSpan(RandomGenerator rng) {
        var builder = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setTraceId(protoId(rng, TRACE_ID_BYTE_LEN))
                .setSpanId(protoId(rng, SPAN_ID_BYTE_LEN))
                .setParentSpanId(protoId(rng, SPAN_ID_BYTE_LEN))
                .setTraceState(asciiString(rng, 10))
                .setFlags((int) uint32(rng))
                .setName(asciiString(rng, 16))
                .setKind(SpanKind.forNumber(SPAN_KIND_NUMBERS[rng.nextInt(SPAN_KIND_NUMBERS.length)]))
                .setStartTimeUnixNano(positiveLong(rng))
                .setEndTimeUnixNano(positiveLong(rng))
                .addAllAttributes(protoKeyValues(rng))
                .setDroppedAttributesCount(rng.nextInt(20))
                .setDroppedEventsCount(rng.nextInt(20))
                .setDroppedLinksCount(rng.nextInt(20))
                .setStatus(io.opentelemetry.proto.trace.v1.Status.newBuilder()
                        .setCode(io.opentelemetry.proto.trace.v1.Status.StatusCode.forNumber(
                                STATUS_CODE_NUMBERS[rng.nextInt(STATUS_CODE_NUMBERS.length)]))
                        .setMessage(asciiString(rng, 20)));
        var eventCount = rng.nextInt(3);
        for (var i = 0; i < eventCount; i++) {
            builder.addEvents(Event.newBuilder()
                    .setTimeUnixNano(positiveLong(rng))
                    .setName(asciiString(rng, 8))
                    .addAllAttributes(protoKeyValues(rng))
                    .setDroppedAttributesCount(rng.nextInt(10)));
        }
        var linkCount = rng.nextInt(3);
        for (var i = 0; i < linkCount; i++) {
            builder.addLinks(Link.newBuilder()
                    .setTraceId(protoId(rng, TRACE_ID_BYTE_LEN))
                    .setSpanId(protoId(rng, SPAN_ID_BYTE_LEN))
                    .setTraceState(asciiString(rng, 10))
                    .addAllAttributes(protoKeyValues(rng))
                    .setDroppedAttributesCount(rng.nextInt(10))
                    .setFlags((int) uint32(rng)));
        }
        return builder.build();
    }

    static ExportTraceServiceRequest protoTraceRequest(RandomGenerator rng) {
        var request = ExportTraceServiceRequest.newBuilder();
        var resourceCount = 1 + rng.nextInt(2);
        for (var r = 0; r < resourceCount; r++) {
            var rsBuilder = ResourceSpans.newBuilder()
                    .setResource(protoResource(rng))
                    .setSchemaUrl(asciiString(rng, 12));
            var scopeCount = 1 + rng.nextInt(2);
            for (var s = 0; s < scopeCount; s++) {
                var ssBuilder = ScopeSpans.newBuilder()
                        .setScope(protoScope(rng))
                        .setSchemaUrl(asciiString(rng, 12));
                var spanCount = 1 + rng.nextInt(3);
                for (var i = 0; i < spanCount; i++) {
                    ssBuilder.addSpans(protoSpan(rng));
                }
                rsBuilder.addScopeSpans(ssBuilder);
            }
            request.addResourceSpans(rsBuilder);
        }
        return request.build();
    }

    // ---- metrics ----

    private static NumberPoint.Value numberValue(RandomGenerator rng) {
        return switch (rng.nextInt(3)) {
            case 0 -> NumberPoint.longValue(rng.nextLong());
            case 1 -> NumberPoint.doubleValue(boundedDouble(rng));
            default -> null;
        };
    }

    private static Exemplar exemplar(RandomGenerator rng) {
        return new Exemplar(
                attributes(rng),
                positiveLong(rng),
                numberValue(rng),
                hexId(rng, SPAN_ID_HEX_LEN),
                hexId(rng, TRACE_ID_HEX_LEN));
    }

    private static List<Exemplar> exemplars(RandomGenerator rng) {
        var n = rng.nextInt(3);
        var result = new ArrayList<Exemplar>(n);
        for (var i = 0; i < n; i++) {
            result.add(exemplar(rng));
        }
        return result;
    }

    private static NumberPoint numberPoint(RandomGenerator rng) {
        return new NumberPoint(
                attributes(rng), positiveLong(rng), positiveLong(rng), numberValue(rng), uint32(rng),
                exemplars(rng));
    }

    private static HistogramPoint histogramPoint(RandomGenerator rng) {
        List<Long> bucketCounts;
        List<Double> explicitBounds;
        if (rng.nextBoolean()) {
            bucketCounts = List.of();
            explicitBounds = List.of();
        } else {
            var boundsSize = rng.nextInt(4);
            var bounds = new ArrayList<Double>(boundsSize);
            for (var i = 0; i < boundsSize; i++) {
                bounds.add(boundedDouble(rng));
            }
            var counts = new ArrayList<Long>(boundsSize + 1);
            for (var i = 0; i <= boundsSize; i++) {
                counts.add(rng.nextLong(0, 1_000));
            }
            bucketCounts = counts;
            explicitBounds = bounds;
        }
        return new HistogramPoint(
                attributes(rng),
                positiveLong(rng),
                positiveLong(rng),
                rng.nextLong(0, 10_000),
                rng.nextBoolean() ? OptionalDouble.of(boundedDouble(rng)) : OptionalDouble.empty(),
                bucketCounts,
                explicitBounds,
                rng.nextBoolean() ? OptionalDouble.of(boundedDouble(rng)) : OptionalDouble.empty(),
                rng.nextBoolean() ? OptionalDouble.of(boundedDouble(rng)) : OptionalDouble.empty(),
                uint32(rng),
                exemplars(rng));
    }

    private static ExponentialHistogramPoint.Buckets buckets(RandomGenerator rng) {
        if (rng.nextBoolean()) {
            return ExponentialHistogramPoint.Buckets.EMPTY;
        }
        var size = rng.nextInt(4);
        var counts = new ArrayList<Long>(size);
        for (var i = 0; i < size; i++) {
            counts.add(rng.nextLong(0, 1_000));
        }
        return new ExponentialHistogramPoint.Buckets(rng.nextInt(-100, 100), counts);
    }

    private static ExponentialHistogramPoint exponentialHistogramPoint(RandomGenerator rng) {
        return new ExponentialHistogramPoint(
                attributes(rng),
                positiveLong(rng),
                positiveLong(rng),
                rng.nextLong(0, 10_000),
                rng.nextBoolean() ? OptionalDouble.of(boundedDouble(rng)) : OptionalDouble.empty(),
                rng.nextInt(-20, 20),
                rng.nextLong(0, 1_000),
                buckets(rng),
                buckets(rng),
                rng.nextBoolean() ? OptionalDouble.of(boundedDouble(rng)) : OptionalDouble.empty(),
                rng.nextBoolean() ? OptionalDouble.of(boundedDouble(rng)) : OptionalDouble.empty(),
                boundedDouble(rng),
                uint32(rng),
                exemplars(rng));
    }

    private static SummaryPoint summaryPoint(RandomGenerator rng) {
        var n = rng.nextInt(4);
        var quantiles = new ArrayList<SummaryPoint.Quantile>(n);
        for (var i = 0; i < n; i++) {
            quantiles.add(new SummaryPoint.Quantile(rng.nextDouble(0, 1), boundedDouble(rng)));
        }
        return new SummaryPoint(
                attributes(rng),
                positiveLong(rng),
                positiveLong(rng),
                rng.nextLong(0, 10_000),
                boundedDouble(rng),
                quantiles,
                uint32(rng));
    }

    private static Metric.AggregationTemporality randomTemporality(RandomGenerator rng) {
        return TEMPORALITIES[rng.nextInt(TEMPORALITIES.length)];
    }

    private static Metric.Data metricData(RandomGenerator rng) {
        return switch (rng.nextInt(6)) {
            case 0 -> new Metric.Gauge(pointList(rng, ModelGen::numberPoint));
            case 1 ->
                new Metric.Sum(
                        pointList(rng, ModelGen::numberPoint), randomTemporality(rng), rng.nextBoolean());
            case 2 ->
                new Metric.Histogram(pointList(rng, ModelGen::histogramPoint), randomTemporality(rng));
            case 3 ->
                new Metric.ExponentialHistogram(
                        pointList(rng, ModelGen::exponentialHistogramPoint), randomTemporality(rng));
            case 4 -> new Metric.Summary(pointList(rng, ModelGen::summaryPoint));
            default -> Metric.noData();
        };
    }

    private static <T> List<T> pointList(RandomGenerator rng, Function<RandomGenerator, T> generator) {
        var n = rng.nextInt(3);
        var result = new ArrayList<T>(n);
        for (var i = 0; i < n; i++) {
            result.add(generator.apply(rng));
        }
        return result;
    }

    private static Metric metric(RandomGenerator rng) {
        return new Metric(
                asciiString(rng, 16), asciiString(rng, 24), asciiString(rng, 6), metricData(rng),
                attributes(rng));
    }

    static MetricsData metricsData(RandomGenerator rng) {
        var resourceCount = 1 + rng.nextInt(2);
        var resourceMetricsList = new ArrayList<MetricsData.ResourceMetrics>(resourceCount);
        for (var r = 0; r < resourceCount; r++) {
            var scopeCount = 1 + rng.nextInt(2);
            var scopeMetricsList = new ArrayList<MetricsData.ScopeMetrics>(scopeCount);
            for (var s = 0; s < scopeCount; s++) {
                var metricCount = 1 + rng.nextInt(3);
                var metrics = new ArrayList<Metric>(metricCount);
                for (var i = 0; i < metricCount; i++) {
                    metrics.add(metric(rng));
                }
                scopeMetricsList.add(
                        new MetricsData.ScopeMetrics(scope(rng), asciiString(rng, 12), metrics));
            }
            resourceMetricsList.add(new MetricsData.ResourceMetrics(
                    resource(rng), asciiString(rng, 12), scopeMetricsList));
        }
        return new MetricsData(resourceMetricsList);
    }

    private static io.opentelemetry.proto.metrics.v1.Exemplar protoExemplar(RandomGenerator rng) {
        var builder = io.opentelemetry.proto.metrics.v1.Exemplar.newBuilder()
                .addAllFilteredAttributes(protoKeyValues(rng))
                .setTimeUnixNano(positiveLong(rng))
                .setSpanId(protoId(rng, SPAN_ID_BYTE_LEN))
                .setTraceId(protoId(rng, TRACE_ID_BYTE_LEN));
        switch (rng.nextInt(3)) {
            case 0 -> builder.setAsInt(rng.nextLong());
            case 1 -> builder.setAsDouble(boundedDouble(rng));
            default -> {
                // value oneof left unset (VALUE_NOT_SET)
            }
        }
        return builder.build();
    }

    private static List<io.opentelemetry.proto.metrics.v1.Exemplar> protoExemplars(RandomGenerator rng) {
        var n = rng.nextInt(3);
        var result = new ArrayList<io.opentelemetry.proto.metrics.v1.Exemplar>(n);
        for (var i = 0; i < n; i++) {
            result.add(protoExemplar(rng));
        }
        return result;
    }

    private static NumberDataPoint protoNumberDataPoint(RandomGenerator rng) {
        var builder = NumberDataPoint.newBuilder()
                .addAllAttributes(protoKeyValues(rng))
                .setStartTimeUnixNano(positiveLong(rng))
                .setTimeUnixNano(positiveLong(rng))
                .setFlags((int) uint32(rng));
        switch (rng.nextInt(3)) {
            case 0 -> builder.setAsInt(rng.nextLong());
            case 1 -> builder.setAsDouble(boundedDouble(rng));
            default -> {
                // value oneof left unset (VALUE_NOT_SET)
            }
        }
        builder.addAllExemplars(protoExemplars(rng));
        return builder.build();
    }

    private static HistogramDataPoint protoHistogramDataPoint(RandomGenerator rng) {
        var builder = HistogramDataPoint.newBuilder()
                .addAllAttributes(protoKeyValues(rng))
                .setStartTimeUnixNano(positiveLong(rng))
                .setTimeUnixNano(positiveLong(rng))
                .setCount(rng.nextLong(0, 10_000))
                .setFlags((int) uint32(rng));
        if (!rng.nextBoolean()) {
            var boundsSize = rng.nextInt(4);
            for (var i = 0; i < boundsSize; i++) {
                builder.addExplicitBounds(boundedDouble(rng));
            }
            for (var i = 0; i <= boundsSize; i++) {
                builder.addBucketCounts(rng.nextLong(0, 1_000));
            }
        }
        if (rng.nextBoolean()) {
            builder.setSum(boundedDouble(rng));
        }
        if (rng.nextBoolean()) {
            builder.setMin(boundedDouble(rng));
        }
        if (rng.nextBoolean()) {
            builder.setMax(boundedDouble(rng));
        }
        builder.addAllExemplars(protoExemplars(rng));
        return builder.build();
    }

    private static ExponentialHistogramDataPoint.Buckets protoBuckets(RandomGenerator rng) {
        var builder =
                ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(rng.nextInt(-100, 100));
        var size = rng.nextInt(4);
        for (var i = 0; i < size; i++) {
            builder.addBucketCounts(rng.nextLong(0, 1_000));
        }
        return builder.build();
    }

    private static ExponentialHistogramDataPoint protoExponentialHistogramDataPoint(
            RandomGenerator rng) {
        var builder = ExponentialHistogramDataPoint.newBuilder()
                .addAllAttributes(protoKeyValues(rng))
                .setStartTimeUnixNano(positiveLong(rng))
                .setTimeUnixNano(positiveLong(rng))
                .setCount(rng.nextLong(0, 10_000))
                .setScale(rng.nextInt(-20, 20))
                .setZeroCount(rng.nextLong(0, 1_000))
                .setPositive(protoBuckets(rng))
                .setNegative(protoBuckets(rng))
                .setZeroThreshold(boundedDouble(rng))
                .setFlags((int) uint32(rng));
        if (rng.nextBoolean()) {
            builder.setSum(boundedDouble(rng));
        }
        if (rng.nextBoolean()) {
            builder.setMin(boundedDouble(rng));
        }
        if (rng.nextBoolean()) {
            builder.setMax(boundedDouble(rng));
        }
        builder.addAllExemplars(protoExemplars(rng));
        return builder.build();
    }

    private static SummaryDataPoint protoSummaryDataPoint(RandomGenerator rng) {
        var builder = SummaryDataPoint.newBuilder()
                .addAllAttributes(protoKeyValues(rng))
                .setStartTimeUnixNano(positiveLong(rng))
                .setTimeUnixNano(positiveLong(rng))
                .setCount(rng.nextLong(0, 10_000))
                .setSum(boundedDouble(rng))
                .setFlags((int) uint32(rng));
        var n = rng.nextInt(4);
        for (var i = 0; i < n; i++) {
            builder.addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder()
                    .setQuantile(rng.nextDouble(0, 1))
                    .setValue(boundedDouble(rng)));
        }
        return builder.build();
    }

    private static AggregationTemporality protoTemporality(RandomGenerator rng) {
        return AggregationTemporality.forNumber(
                TEMPORALITY_NUMBERS[rng.nextInt(TEMPORALITY_NUMBERS.length)]);
    }

    private static io.opentelemetry.proto.metrics.v1.Metric protoMetric(RandomGenerator rng) {
        var builder = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setName(asciiString(rng, 16))
                .setDescription(asciiString(rng, 24))
                .setUnit(asciiString(rng, 6))
                .addAllMetadata(protoKeyValues(rng));
        switch (rng.nextInt(6)) {
            case 0 -> {
                var gauge = Gauge.newBuilder();
                var n = rng.nextInt(3);
                for (var i = 0; i < n; i++) {
                    gauge.addDataPoints(protoNumberDataPoint(rng));
                }
                builder.setGauge(gauge);
            }
            case 1 -> {
                var sum = Sum.newBuilder()
                        .setAggregationTemporality(protoTemporality(rng))
                        .setIsMonotonic(rng.nextBoolean());
                var n = rng.nextInt(3);
                for (var i = 0; i < n; i++) {
                    sum.addDataPoints(protoNumberDataPoint(rng));
                }
                builder.setSum(sum);
            }
            case 2 -> {
                var histogram = Histogram.newBuilder().setAggregationTemporality(protoTemporality(rng));
                var n = rng.nextInt(3);
                for (var i = 0; i < n; i++) {
                    histogram.addDataPoints(protoHistogramDataPoint(rng));
                }
                builder.setHistogram(histogram);
            }
            case 3 -> {
                var histogram =
                        ExponentialHistogram.newBuilder().setAggregationTemporality(protoTemporality(rng));
                var n = rng.nextInt(3);
                for (var i = 0; i < n; i++) {
                    histogram.addDataPoints(protoExponentialHistogramDataPoint(rng));
                }
                builder.setExponentialHistogram(histogram);
            }
            case 4 -> {
                var summary = Summary.newBuilder();
                var n = rng.nextInt(3);
                for (var i = 0; i < n; i++) {
                    summary.addDataPoints(protoSummaryDataPoint(rng));
                }
                builder.setSummary(summary);
            }
            default -> {
                // data oneof left unset (DATA_NOT_SET)
            }
        }
        return builder.build();
    }

    static ExportMetricsServiceRequest protoMetricsRequest(RandomGenerator rng) {
        var request = ExportMetricsServiceRequest.newBuilder();
        var resourceCount = 1 + rng.nextInt(2);
        for (var r = 0; r < resourceCount; r++) {
            var rmBuilder = ResourceMetrics.newBuilder()
                    .setResource(protoResource(rng))
                    .setSchemaUrl(asciiString(rng, 12));
            var scopeCount = 1 + rng.nextInt(2);
            for (var s = 0; s < scopeCount; s++) {
                var smBuilder = ScopeMetrics.newBuilder()
                        .setScope(protoScope(rng))
                        .setSchemaUrl(asciiString(rng, 12));
                var metricCount = 1 + rng.nextInt(3);
                for (var i = 0; i < metricCount; i++) {
                    smBuilder.addMetrics(protoMetric(rng));
                }
                rmBuilder.addScopeMetrics(smBuilder);
            }
            request.addResourceMetrics(rmBuilder);
        }
        return request.build();
    }

    // ---- logs ----

    private static LogRecord logRecord(RandomGenerator rng) {
        return LogRecord.builder()
                .epochNanos(positiveLong(rng))
                .observedEpochNanos(positiveLong(rng))
                .severity(SEVERITIES[rng.nextInt(SEVERITIES.length)])
                .severityText(asciiString(rng, 8))
                .body(attributeValue(rng, 0))
                .attributes(attributes(rng))
                .droppedAttributesCount(rng.nextInt(20))
                .flags(uint32(rng))
                .traceId(hexId(rng, TRACE_ID_HEX_LEN))
                .spanId(hexId(rng, SPAN_ID_HEX_LEN))
                .eventName(asciiString(rng, 12))
                .build();
    }

    static LogsData logsData(RandomGenerator rng) {
        var resourceCount = 1 + rng.nextInt(2);
        var resourceLogsList = new ArrayList<LogsData.ResourceLogs>(resourceCount);
        for (var r = 0; r < resourceCount; r++) {
            var scopeCount = 1 + rng.nextInt(2);
            var scopeLogsList = new ArrayList<LogsData.ScopeLogs>(scopeCount);
            for (var s = 0; s < scopeCount; s++) {
                var recordCount = 1 + rng.nextInt(3);
                var records = new ArrayList<LogRecord>(recordCount);
                for (var i = 0; i < recordCount; i++) {
                    records.add(logRecord(rng));
                }
                scopeLogsList.add(new LogsData.ScopeLogs(scope(rng), asciiString(rng, 12), records));
            }
            resourceLogsList.add(
                    new LogsData.ResourceLogs(resource(rng), asciiString(rng, 12), scopeLogsList));
        }
        return new LogsData(resourceLogsList);
    }

    private static io.opentelemetry.proto.logs.v1.LogRecord protoLogRecord(RandomGenerator rng) {
        return io.opentelemetry.proto.logs.v1.LogRecord.newBuilder()
                .setTimeUnixNano(positiveLong(rng))
                .setObservedTimeUnixNano(positiveLong(rng))
                .setSeverityNumber(
                        SeverityNumber.forNumber(SEVERITY_NUMBERS[rng.nextInt(SEVERITY_NUMBERS.length)]))
                .setSeverityText(asciiString(rng, 8))
                .setBody(protoAnyValue(rng, 0))
                .addAllAttributes(protoKeyValues(rng))
                .setDroppedAttributesCount(rng.nextInt(20))
                .setFlags((int) uint32(rng))
                .setTraceId(protoId(rng, TRACE_ID_BYTE_LEN))
                .setSpanId(protoId(rng, SPAN_ID_BYTE_LEN))
                .setEventName(asciiString(rng, 12))
                .build();
    }

    static ExportLogsServiceRequest protoLogsRequest(RandomGenerator rng) {
        var request = ExportLogsServiceRequest.newBuilder();
        var resourceCount = 1 + rng.nextInt(2);
        for (var r = 0; r < resourceCount; r++) {
            var rlBuilder = ResourceLogs.newBuilder()
                    .setResource(protoResource(rng))
                    .setSchemaUrl(asciiString(rng, 12));
            var scopeCount = 1 + rng.nextInt(2);
            for (var s = 0; s < scopeCount; s++) {
                var slBuilder = ScopeLogs.newBuilder()
                        .setScope(protoScope(rng))
                        .setSchemaUrl(asciiString(rng, 12));
                var recordCount = 1 + rng.nextInt(3);
                for (var i = 0; i < recordCount; i++) {
                    slBuilder.addLogRecords(protoLogRecord(rng));
                }
                rlBuilder.addScopeLogs(slBuilder);
            }
            request.addResourceLogs(rlBuilder);
        }
        return request.build();
    }

    // ---- profiles ----

    /// A scalar-only domain profile (empty `rawProfile`) with `sampleCount` fixed at zero, per the
    /// design's section 4.3: the mapper's scalar rebuild path never emits samples, so it cannot
    /// reconstruct a nonzero sample count, and fixing it here keeps the general round trip
    /// assertion exact instead of special-casing a field comparator.
    static ProfilesData.Profile scalarProfile(RandomGenerator rng) {
        return new ProfilesData.Profile(
                hexId(rng, TRACE_ID_HEX_LEN),
                positiveLong(rng),
                positiveLong(rng),
                positiveLong(rng),
                0,
                rng.nextInt(20),
                asciiString(rng, 8),
                new byte[0]);
    }

    private static ValueType protoValueType(RandomGenerator rng) {
        return ValueType.newBuilder()
                .setTypeStrindex(rng.nextInt(10))
                .setUnitStrindex(rng.nextInt(10))
                .build();
    }

    private static Profile protoRichProfile(RandomGenerator rng) {
        var builder = Profile.newBuilder()
                .setSampleType(protoValueType(rng))
                .setTimeUnixNano(positiveLong(rng))
                .setDurationNano(positiveLong(rng))
                .setPeriodType(protoValueType(rng))
                .setPeriod(rng.nextLong())
                .setProfileId(protoId(rng, TRACE_ID_BYTE_LEN))
                .setDroppedAttributesCount(rng.nextInt(20))
                .setOriginalPayloadFormat(asciiString(rng, 6))
                .setOriginalPayload(ByteString.copyFrom(randomBytes(rng, rng.nextInt(16))));
        var sampleCount = 1 + rng.nextInt(3);
        for (var i = 0; i < sampleCount; i++) {
            var sample = Sample.newBuilder().setStackIndex(rng.nextInt(10)).setLinkIndex(rng.nextInt(10));
            var valueCount = 1 + rng.nextInt(3);
            for (var v = 0; v < valueCount; v++) {
                sample.addValues(rng.nextLong());
            }
            var attrCount = rng.nextInt(3);
            for (var a = 0; a < attrCount; a++) {
                sample.addAttributeIndices(rng.nextInt(10));
            }
            builder.addSamples(sample);
        }
        var attrIndexCount = rng.nextInt(3);
        for (var i = 0; i < attrIndexCount; i++) {
            builder.addAttributeIndices(rng.nextInt(10));
        }
        return builder.build();
    }

    private static ProfilesDictionary protoDictionary(RandomGenerator rng) {
        var builder = ProfilesDictionary.newBuilder();
        var stringCount = 1 + rng.nextInt(4);
        for (var i = 0; i < stringCount; i++) {
            builder.addStringTable(asciiString(rng, 8));
        }
        var attrCount = rng.nextInt(3);
        for (var i = 0; i < attrCount; i++) {
            builder.addAttributeTable(KeyValueAndUnit.newBuilder()
                    .setKeyStrindex(rng.nextInt(stringCount))
                    .setValue(protoAnyValue(rng, 0)));
        }
        return builder.build();
    }

    /// A full profiles export request with real samples and a shared dictionary, exercising the
    /// lossless opaque-passthrough path (section 4.3): every value here is randomized, but the
    /// request is always a well-formed proto payload, exactly like the wire bytes a real profiler
    /// would send.
    static ExportProfilesServiceRequest protoProfilesRequestRich(RandomGenerator rng) {
        var profile = protoRichProfile(rng);
        return ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder()
                        .setResource(protoResource(rng))
                        .setSchemaUrl(asciiString(rng, 12))
                        .addScopeProfiles(ScopeProfiles.newBuilder()
                                .setScope(protoScope(rng))
                                .setSchemaUrl(asciiString(rng, 12))
                                .addProfiles(profile)))
                .setDictionary(protoDictionary(rng))
                .build();
    }
}
