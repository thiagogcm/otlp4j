package dev.nthings.otlp4j.opentelemetry;

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
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.model.TracesData;
import io.opentelemetry.api.common.KeyValue;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.ExemplarData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongExemplarData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/** Converts immutable OpenTelemetry SDK export data into immutable otlp4j batches. */
final class SdkDataMapper {

    // OTLP SpanFlags uses the low byte for W3C trace flags and the next two bits to encode whether
    // a parent/link context is known to be remote. These are the values used by the official SDK
    // OTLP exporter when it snapshots SpanData and LinkData.
    private static final long CONTEXT_HAS_IS_REMOTE_MASK = 0x00000100L;
    private static final long CONTEXT_IS_REMOTE_MASK = 0x00000200L;

    private SdkDataMapper() {}

    static TracesData traces(Collection<SpanData> spans) {
        var groups = new LinkedHashMap<ResourceKey, ResourceGroup<Span>>();
        for (var sdkSpan : spans) {
            var sdkResource = sdkSpan.getResource();
            var resource = resourceOf(sdkResource);
            var resourceSchemaUrl = schemaUrl(sdkResource.getSchemaUrl());
            var resourceKey = new ResourceKey(resource, resourceSchemaUrl);
            var resourceGroup = groups.computeIfAbsent(resourceKey, key -> new ResourceGroup<>(
                    key.resource, key.schemaUrl));
            var sdkScope = sdkSpan.getInstrumentationScopeInfo();
            var scope = scopeOf(sdkScope);
            var scopeSchemaUrl = schemaUrl(sdkScope.getSchemaUrl());
            var scopeKey = new ScopeKey(scope, scopeSchemaUrl);
            var scopeGroup = resourceGroup.scopes.computeIfAbsent(scopeKey, key ->
                    new ScopeGroup<>(key.scope, key.schemaUrl));
            scopeGroup.items.add(spanOf(sdkSpan));
        }

        var resourceSpans = new ArrayList<TracesData.ResourceSpans>(groups.size());
        for (var resourceGroup : groups.values()) {
            var scopeSpans = new ArrayList<TracesData.ScopeSpans>(resourceGroup.scopes.size());
            for (var scopeGroup : resourceGroup.scopes.values()) {
                scopeSpans.add(new TracesData.ScopeSpans(
                        scopeGroup.scope, scopeGroup.schemaUrl, scopeGroup.items));
            }
            resourceSpans.add(new TracesData.ResourceSpans(
                    resourceGroup.resource, resourceGroup.schemaUrl, scopeSpans));
        }
        return new TracesData(resourceSpans);
    }

    static LogsData logs(Collection<LogRecordData> logs) {
        var groups = new LinkedHashMap<ResourceKey, ResourceGroup<LogRecord>>();
        for (var sdkLog : logs) {
            var sdkResource = sdkLog.getResource();
            var resource = resourceOf(sdkResource);
            var resourceSchemaUrl = schemaUrl(sdkResource.getSchemaUrl());
            var resourceKey = new ResourceKey(resource, resourceSchemaUrl);
            var resourceGroup = groups.computeIfAbsent(resourceKey, key -> new ResourceGroup<>(
                    key.resource, key.schemaUrl));
            var sdkScope = sdkLog.getInstrumentationScopeInfo();
            var scope = scopeOf(sdkScope);
            var scopeSchemaUrl = schemaUrl(sdkScope.getSchemaUrl());
            var scopeKey = new ScopeKey(scope, scopeSchemaUrl);
            var scopeGroup = resourceGroup.scopes.computeIfAbsent(scopeKey, key ->
                    new ScopeGroup<>(key.scope, key.schemaUrl));
            scopeGroup.items.add(logOf(sdkLog));
        }

        var resourceLogs = new ArrayList<LogsData.ResourceLogs>(groups.size());
        for (var resourceGroup : groups.values()) {
            var scopeLogs = new ArrayList<LogsData.ScopeLogs>(resourceGroup.scopes.size());
            for (var scopeGroup : resourceGroup.scopes.values()) {
                scopeLogs.add(new LogsData.ScopeLogs(
                        scopeGroup.scope, scopeGroup.schemaUrl, scopeGroup.items));
            }
            resourceLogs.add(new LogsData.ResourceLogs(
                    resourceGroup.resource, resourceGroup.schemaUrl, scopeLogs));
        }
        return new LogsData(resourceLogs);
    }

    static MetricsData metrics(Collection<MetricData> metrics) {
        var groups = new LinkedHashMap<ResourceKey, ResourceGroup<Metric>>();
        for (var sdkMetric : metrics) {
            var sdkResource = sdkMetric.getResource();
            var resource = resourceOf(sdkResource);
            var resourceSchemaUrl = schemaUrl(sdkResource.getSchemaUrl());
            var resourceKey = new ResourceKey(resource, resourceSchemaUrl);
            var resourceGroup = groups.computeIfAbsent(resourceKey, key -> new ResourceGroup<>(
                    key.resource, key.schemaUrl));
            var sdkScope = sdkMetric.getInstrumentationScopeInfo();
            var scope = scopeOf(sdkScope);
            var scopeSchemaUrl = schemaUrl(sdkScope.getSchemaUrl());
            var scopeKey = new ScopeKey(scope, scopeSchemaUrl);
            var scopeGroup = resourceGroup.scopes.computeIfAbsent(scopeKey, key ->
                    new ScopeGroup<>(key.scope, key.schemaUrl));
            scopeGroup.items.add(metricOf(sdkMetric));
        }

        var resourceMetrics = new ArrayList<MetricsData.ResourceMetrics>(groups.size());
        for (var resourceGroup : groups.values()) {
            var scopeMetrics = new ArrayList<MetricsData.ScopeMetrics>(resourceGroup.scopes.size());
            for (var scopeGroup : resourceGroup.scopes.values()) {
                scopeMetrics.add(new MetricsData.ScopeMetrics(
                        scopeGroup.scope, scopeGroup.schemaUrl, scopeGroup.items));
            }
            resourceMetrics.add(new MetricsData.ResourceMetrics(
                    resourceGroup.resource, resourceGroup.schemaUrl, scopeMetrics));
        }
        return new MetricsData(resourceMetrics);
    }

    private static Span spanOf(SpanData sdkSpan) {
        // SpanData.hasEnded() is SDK lifecycle state; OTLP Span has no corresponding field.
        var context = sdkSpan.getSpanContext();
        var parent = sdkSpan.getParentSpanContext();
        var events = new ArrayList<Span.Event>(sdkSpan.getEvents().size());
        for (var event : sdkSpan.getEvents()) {
            events.add(new Span.Event(
                    event.getEpochNanos(),
                    event.getName(),
                    attributesOf(event.getAttributes()),
                    dropped(event.getTotalAttributeCount(), event.getAttributes().size())));
        }
        var links = new ArrayList<Span.Link>(sdkSpan.getLinks().size());
        for (var link : sdkSpan.getLinks()) {
            var linkContext = link.getSpanContext();
            links.add(new Span.Link(
                    traceId(linkContext),
                    spanId(linkContext),
                    traceState(linkContext),
                    attributesOf(link.getAttributes()),
                    dropped(link.getTotalAttributeCount(), link.getAttributes().size()),
                    contextFlags(linkContext)));
        }
        return new Span(
                traceId(context),
                spanId(context),
                spanId(parent),
                traceState(context),
                spanFlags(context, parent),
                sdkSpan.getName(),
                spanKind(sdkSpan.getKind()),
                sdkSpan.getStartEpochNanos(),
                sdkSpan.getEndEpochNanos(),
                attributesOf(sdkSpan.getAttributes()),
                dropped(sdkSpan.getTotalAttributeCount(), sdkSpan.getAttributes().size()),
                events,
                dropped(sdkSpan.getTotalRecordedEvents(), sdkSpan.getEvents().size()),
                links,
                dropped(sdkSpan.getTotalRecordedLinks(), sdkSpan.getLinks().size()),
                statusOf(sdkSpan.getStatus()));
    }

    private static LogRecord logOf(LogRecordData sdkLog) {
        var context = sdkLog.getSpanContext();
        var severity = sdkLog.getSeverity();
        var body = sdkLog.getBodyValue();
        return new LogRecord(
                sdkLog.getTimestampEpochNanos(),
                sdkLog.getObservedTimestampEpochNanos(),
                LogRecord.Severity.fromNumber(severity == null ? 0 : severity.getSeverityNumber()),
                sdkLog.getSeverityText() == null ? "" : sdkLog.getSeverityText(),
                body == null ? AttributeValue.empty() : attributeValueOf(body),
                attributesOf(sdkLog.getAttributes()),
                dropped(sdkLog.getTotalAttributeCount(), sdkLog.getAttributes().size()),
                traceFlags(context),
                traceId(context),
                spanId(context),
                sdkLog.getEventName() == null ? "" : sdkLog.getEventName());
    }

    private static Metric metricOf(MetricData sdkMetric) {
        var data = switch (sdkMetric.getType()) {
            case LONG_GAUGE -> new Metric.Gauge(longPoints(sdkMetric.getLongGaugeData().getPoints()));
            case DOUBLE_GAUGE -> new Metric.Gauge(doublePoints(sdkMetric.getDoubleGaugeData().getPoints()));
            case LONG_SUM -> {
                var sum = sdkMetric.getLongSumData();
                yield new Metric.Sum(
                        longPoints(sum.getPoints()),
                        temporality(sum.getAggregationTemporality()),
                        sum.isMonotonic());
            }
            case DOUBLE_SUM -> {
                var sum = sdkMetric.getDoubleSumData();
                yield new Metric.Sum(
                        doublePoints(sum.getPoints()),
                        temporality(sum.getAggregationTemporality()),
                        sum.isMonotonic());
            }
            case HISTOGRAM -> {
                var histogram = sdkMetric.getHistogramData();
                yield new Metric.Histogram(
                        histogramPoints(histogram.getPoints()),
                        temporality(histogram.getAggregationTemporality()));
            }
            case EXPONENTIAL_HISTOGRAM -> {
                var histogram = sdkMetric.getExponentialHistogramData();
                yield new Metric.ExponentialHistogram(
                        exponentialHistogramPoints(histogram.getPoints()),
                        temporality(histogram.getAggregationTemporality()));
            }
            case SUMMARY -> new Metric.Summary(summaryPoints(sdkMetric.getSummaryData().getPoints()));
        };
        // MetricData has no metadata field. The model's metadata is reserved for OTLP metadata
        // emitted by wire producers, so an SDK export correctly carries an empty collection.
        return new Metric(
                sdkMetric.getName(), sdkMetric.getDescription(), sdkMetric.getUnit(), data,
                Attributes.empty());
    }

    private static List<NumberPoint> longPoints(Collection<LongPointData> points) {
        var result = new ArrayList<NumberPoint>(points.size());
        for (var point : points) {
            result.add(new NumberPoint(
                    attributesOf(point.getAttributes()),
                    point.getStartEpochNanos(),
                    point.getEpochNanos(),
                    NumberPoint.longValue(point.getValue()),
                    0L, // SDK PointData exposes no OTLP data-point flags.
                    exemplars(point.getExemplars())));
        }
        return result;
    }

    private static List<NumberPoint> doublePoints(Collection<DoublePointData> points) {
        var result = new ArrayList<NumberPoint>(points.size());
        for (var point : points) {
            result.add(new NumberPoint(
                    attributesOf(point.getAttributes()),
                    point.getStartEpochNanos(),
                    point.getEpochNanos(),
                    NumberPoint.doubleValue(point.getValue()),
                    0L, // SDK PointData exposes no OTLP data-point flags.
                    exemplars(point.getExemplars())));
        }
        return result;
    }

    private static List<HistogramPoint> histogramPoints(Collection<HistogramPointData> points) {
        var result = new ArrayList<HistogramPoint>(points.size());
        for (var point : points) {
            result.add(new HistogramPoint(
                    attributesOf(point.getAttributes()),
                    point.getStartEpochNanos(),
                    point.getEpochNanos(),
                    point.getCount(),
                    OptionalDouble.of(point.getSum()),
                    point.getCounts(),
                    point.getBoundaries(),
                    point.hasMin() ? OptionalDouble.of(point.getMin()) : OptionalDouble.empty(),
                    point.hasMax() ? OptionalDouble.of(point.getMax()) : OptionalDouble.empty(),
                    0L, // SDK PointData exposes no OTLP data-point flags.
                    exemplars(point.getExemplars())));
        }
        return result;
    }

    private static List<ExponentialHistogramPoint> exponentialHistogramPoints(
            Collection<ExponentialHistogramPointData> points) {
        var result = new ArrayList<ExponentialHistogramPoint>(points.size());
        for (var point : points) {
            var positive = point.getPositiveBuckets();
            var negative = point.getNegativeBuckets();
            result.add(new ExponentialHistogramPoint(
                    attributesOf(point.getAttributes()),
                    point.getStartEpochNanos(),
                    point.getEpochNanos(),
                    point.getCount(),
                    OptionalDouble.of(point.getSum()),
                    point.getScale(),
                    point.getZeroCount(),
                    new ExponentialHistogramPoint.Buckets(
                            positive.getOffset(), positive.getBucketCounts()),
                    new ExponentialHistogramPoint.Buckets(
                            negative.getOffset(), negative.getBucketCounts()),
                    point.hasMin() ? OptionalDouble.of(point.getMin()) : OptionalDouble.empty(),
                    point.hasMax() ? OptionalDouble.of(point.getMax()) : OptionalDouble.empty(),
                    0D, // SDK 1.65 exposes no zero-threshold accessor.
                    0L, // SDK PointData exposes no OTLP data-point flags.
                    exemplars(point.getExemplars())));
        }
        return result;
    }

    private static List<SummaryPoint> summaryPoints(Collection<SummaryPointData> points) {
        var result = new ArrayList<SummaryPoint>(points.size());
        for (var point : points) {
            var quantiles = new ArrayList<SummaryPoint.Quantile>(point.getValues().size());
            for (var value : point.getValues()) {
                quantiles.add(new SummaryPoint.Quantile(value.getQuantile(), value.getValue()));
            }
            result.add(new SummaryPoint(
                    attributesOf(point.getAttributes()),
                    point.getStartEpochNanos(),
                    point.getEpochNanos(),
                    point.getCount(),
                    point.getSum(),
                    quantiles,
                    0L)); // SDK PointData exposes no OTLP data-point flags.
        }
        return result;
    }

    private static List<Exemplar> exemplars(List<? extends ExemplarData> sdkExemplars) {
        if (sdkExemplars.isEmpty()) {
            return List.of();
        }
        var result = new ArrayList<Exemplar>(sdkExemplars.size());
        for (var exemplar : sdkExemplars) {
            var value = switch (exemplar) {
                case LongExemplarData longExemplar -> NumberPoint.longValue(longExemplar.getValue());
                case DoubleExemplarData doubleExemplar -> NumberPoint.doubleValue(doubleExemplar.getValue());
                default -> throw new IllegalArgumentException(
                        "Unsupported OpenTelemetry exemplar type: " + exemplar.getClass().getName());
            };
            var context = exemplar.getSpanContext();
            // The SDK exemplar contract exposes a SpanContext, while the OTLP Exemplar model
            // carries only its trace/span IDs; trace-state and context flags have no exemplar
            // fields to preserve.
            result.add(new Exemplar(
                    attributesOf(exemplar.getFilteredAttributes()),
                    exemplar.getEpochNanos(),
                    value,
                    spanId(context),
                    traceId(context)));
        }
        return result;
    }

    private static Attributes attributesOf(io.opentelemetry.api.common.Attributes sdkAttributes) {
        if (sdkAttributes == null || sdkAttributes.isEmpty()) {
            return Attributes.empty();
        }
        var values = new LinkedHashMap<String, AttributeValue>(sdkAttributes.size());
        sdkAttributes.forEach((key, value) -> values.put(key.getKey(), attributeValueOfAttribute(value)));
        return Attributes.of(values);
    }

    private static AttributeValue attributeValueOfAttribute(Object value) {
        if (value instanceof Value<?> sdkValue) {
            return attributeValueOf(sdkValue);
        }
        if (value instanceof String string) {
            return AttributeValue.of(string);
        }
        if (value instanceof Boolean bool) {
            return AttributeValue.of(bool);
        }
        if (value instanceof Long number) {
            return AttributeValue.of(number);
        }
        if (value instanceof Double number) {
            return AttributeValue.of(number);
        }
        if (value instanceof List<?> list) {
            var values = new ArrayList<AttributeValue>(list.size());
            for (var item : list) {
                values.add(attributeValueOfAttribute(item));
            }
            return AttributeValue.of(values);
        }
        if (value instanceof byte[] bytes) {
            return AttributeValue.of(bytes);
        }
        if (value instanceof ByteBuffer bytes) {
            var copy = bytes.asReadOnlyBuffer();
            var bytesArray = new byte[copy.remaining()];
            copy.get(bytesArray);
            return AttributeValue.of(bytesArray);
        }
        throw new IllegalArgumentException("Unsupported OpenTelemetry attribute value: " + value);
    }

    private static AttributeValue attributeValueOf(Value<?> value) {
        return switch (value.getType()) {
            case STRING -> AttributeValue.of((String) value.getValue());
            case BOOLEAN -> AttributeValue.of((Boolean) value.getValue());
            case LONG -> AttributeValue.of((Long) value.getValue());
            case DOUBLE -> AttributeValue.of((Double) value.getValue());
            case BYTES -> {
                var bytes = ((ByteBuffer) value.getValue()).asReadOnlyBuffer();
                var copy = new byte[bytes.remaining()];
                bytes.get(copy);
                yield AttributeValue.of(copy);
            }
            case ARRAY -> {
                var values = new ArrayList<AttributeValue>();
                for (var element : (List<?>) value.getValue()) {
                    if (!(element instanceof Value<?> nestedValue)) {
                        throw new IllegalArgumentException("OpenTelemetry array contains a non-Value element");
                    }
                    values.add(attributeValueOf(nestedValue));
                }
                yield AttributeValue.of(values);
            }
            case KEY_VALUE_LIST -> {
                var values = new LinkedHashMap<String, AttributeValue>();
                for (var element : (List<?>) value.getValue()) {
                    if (!(element instanceof KeyValue keyValue)) {
                        throw new IllegalArgumentException("OpenTelemetry key-value list contains a non-KeyValue element");
                    }
                    values.put(keyValue.getKey(), attributeValueOf(keyValue.getValue()));
                }
                yield AttributeValue.of(values);
            }
            case EMPTY -> AttributeValue.empty();
        };
    }

    private static Resource resourceOf(io.opentelemetry.sdk.resources.Resource resource) {
        // SDK Resource has attributes and a schema URL, but no dropped-attribute count.
        return new Resource(attributesOf(resource.getAttributes()), 0);
    }

    private static InstrumentationScope scopeOf(InstrumentationScopeInfo scope) {
        // SDK InstrumentationScopeInfo has attributes and a schema URL, but no dropped-attribute
        // count.
        return new InstrumentationScope(
                scope.getName(),
                scope.getVersion() == null ? "" : scope.getVersion(),
                attributesOf(scope.getAttributes()),
                0);
    }

    private static Span.Kind spanKind(SpanKind kind) {
        return switch (kind) {
            case INTERNAL -> Span.Kind.INTERNAL;
            case SERVER -> Span.Kind.SERVER;
            case CLIENT -> Span.Kind.CLIENT;
            case PRODUCER -> Span.Kind.PRODUCER;
            case CONSUMER -> Span.Kind.CONSUMER;
        };
    }

    private static Span.Status statusOf(StatusData status) {
        var code = switch (status.getStatusCode()) {
            case UNSET -> Span.Status.Code.UNSET;
            case OK -> Span.Status.Code.OK;
            case ERROR -> Span.Status.Code.ERROR;
        };
        return new Span.Status(code, status.getDescription());
    }

    private static Metric.AggregationTemporality temporality(
            AggregationTemporality temporality) {
        if (temporality == null) {
            return Metric.AggregationTemporality.UNSPECIFIED;
        }
        return switch (temporality) {
            case DELTA -> Metric.AggregationTemporality.DELTA;
            case CUMULATIVE -> Metric.AggregationTemporality.CUMULATIVE;
        };
    }

    private static int dropped(int total, int retained) {
        return Math.max(0, total - retained);
    }

    private static String schemaUrl(String schemaUrl) {
        return schemaUrl == null ? "" : schemaUrl;
    }

    private static String traceId(SpanContext context) {
        return context != null && context.isValid() ? context.getTraceId() : "";
    }

    private static String spanId(SpanContext context) {
        return context != null && context.isValid() ? context.getSpanId() : "";
    }

    private static String traceState(SpanContext context) {
        return context == null ? "" : context.getTraceState().asMap().entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private static long traceFlags(SpanContext context) {
        return context == null ? 0L : Byte.toUnsignedInt(context.getTraceFlags().asByte());
    }

    private static long spanFlags(SpanContext spanContext, SpanContext parentContext) {
        if (parentContext == null) {
            // A null parent is outside the SDK SpanData contract, so preserve only the known
            // trace flags rather than manufacturing a remote-state assertion.
            return traceFlags(spanContext);
        }
        return contextFlags(spanContext)
                | (parentContext.isRemote() ? CONTEXT_IS_REMOTE_MASK : 0L);
    }

    private static long contextFlags(SpanContext context) {
        if (context == null) {
            return 0L;
        }
        return traceFlags(context) | CONTEXT_HAS_IS_REMOTE_MASK
                | (context.isRemote() ? CONTEXT_IS_REMOTE_MASK : 0L);
    }

    private record ResourceKey(Resource resource, String schemaUrl) {}

    private record ScopeKey(InstrumentationScope scope, String schemaUrl) {}

    private static final class ResourceGroup<T> {
        private final Resource resource;
        private final String schemaUrl;
        private final Map<ScopeKey, ScopeGroup<T>> scopes = new LinkedHashMap<>();

        private ResourceGroup(Resource resource, String schemaUrl) {
            this.resource = resource;
            this.schemaUrl = schemaUrl;
        }
    }

    private static final class ScopeGroup<T> {
        private final InstrumentationScope scope;
        private final String schemaUrl;
        private final List<T> items = new ArrayList<>();

        private ScopeGroup(InstrumentationScope scope, String schemaUrl) {
            this.scope = scope;
            this.schemaUrl = schemaUrl;
        }
    }
}
