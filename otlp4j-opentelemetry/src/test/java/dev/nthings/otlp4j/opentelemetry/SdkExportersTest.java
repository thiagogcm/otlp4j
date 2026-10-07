package dev.nthings.otlp4j.opentelemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Metric;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramBuckets;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.SummaryData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.data.ValueAtQuantile;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class SdkExportersTest {

    private static final String TRACE_ID = "0102030405060708090a0b0c0d0e0f10";
    private static final String SPAN_ID = "0102030405060708";
    private static final String PARENT_ID = "1112131415161718";
    private static final SpanContext CONTEXT = SpanContext.create(
            TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.builder().put("vendor", "value").build());
    private static final SpanContext REMOTE_PARENT = SpanContext.createFromRemoteParent(
            TRACE_ID, PARENT_ID, TraceFlags.getSampled(), TraceState.getDefault());
    private static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.builder("orders-lib")
            .setVersion("2.1")
            .setSchemaUrl("https://schema.example/orders")
            .setAttributes(Attributes.of(AttributeKey.stringKey("scope.key"), "scope.value"))
            .build();

    @Test
    void groupingKeepsSchemaVersionsWithTheirSpansAndCoalescesEquivalentSources() {
        var attributes = Attributes.of(AttributeKey.stringKey("service.name"), "checkout");
        var resourceV1 = Resource.create(attributes, "https://schema.example/resource/1");
        var resourceV2 = Resource.create(attributes, "https://schema.example/resource/2");
        var scopeV1 = InstrumentationScopeInfo.builder("orders-lib")
                .setSchemaUrl("https://schema.example/scope/1").build();
        var scopeV2 = InstrumentationScopeInfo.builder("orders-lib")
                .setSchemaUrl("https://schema.example/scope/2").build();
        var equivalentResource = Resource.create(attributes, "https://schema.example/resource/1");
        var equivalentScope = InstrumentationScopeInfo.builder("orders-lib")
                .setSchemaUrl("https://schema.example/scope/1").build();
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        var exporter = SdkExporters.spans(input);

        assertThat(exporter.export(List.of(
                spanForGroup("pay", resourceV1, scopeV1),
                spanForGroup("fetch", resourceV2, scopeV1),
                spanForGroup("ship", resourceV1, scopeV2),
                spanForGroup("refund", equivalentResource, equivalentScope))).isSuccess()).isTrue();

        var groups = input.onlyBatch().resourceSpans();
        assertThat(groups).extracting(group -> group.schemaUrl()).containsExactly(
                "https://schema.example/resource/1", "https://schema.example/resource/2");
        assertThat(groups.getFirst().scopeSpans()).extracting(group -> group.schemaUrl()).containsExactly(
                "https://schema.example/scope/1", "https://schema.example/scope/2");
        assertThat(groups.getFirst().scopeSpans().getFirst().spans())
                .extracting(span -> span.name()).containsExactly("pay", "refund");
        assertThat(groups.getFirst().scopeSpans().get(1).spans())
                .extracting(span -> span.name()).containsExactly("ship");
        assertThat(groups.get(1).scopeSpans()).singleElement().satisfies(scope -> {
            assertThat(scope.schemaUrl()).isEqualTo("https://schema.example/scope/1");
            assertThat(scope.spans()).extracting(span -> span.name()).containsExactly("fetch");
        });
    }

    private static SpanData spanForGroup(String name, Resource resource, InstrumentationScopeInfo scope) {
        var context = SpanContext.create(TRACE_ID, "%016x".formatted(name.hashCode()),
                TraceFlags.getSampled(), TraceState.getDefault());
        return new TestSpanData(name, SpanKind.INTERNAL, context, SpanContext.getInvalid(), StatusData.unset(),
                1L, 2L, Attributes.empty(), List.of(), List.of(), true, 0, 0, 0, scope, resource);
    }

    @Test
    void spanExportSnapshotsTraceFieldsAndSchemaMetadata() {
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        var exporter = SdkExporters.spans(input);
        var event = EventData.create(150L, "annotated", Attributes.of(AttributeKey.longKey("count"), 2L), 4);
        var link = LinkData.create(
                REMOTE_PARENT,
                Attributes.of(AttributeKey.stringKey("link.key"), "link.value"),
                3);
        var sdkSpan = new TestSpanData(
                "checkout",
                SpanKind.SERVER,
                CONTEXT,
                REMOTE_PARENT,
                StatusData.create(io.opentelemetry.api.trace.StatusCode.ERROR, "failed"),
                100L,
                200L,
                Attributes.builder()
                        .put(AttributeKey.stringKey("span.key"), "span.value")
                        .put(AttributeKey.valueKey("span.body"), Value.of(Map.of("ok", Value.of(true))))
                        .build(),
                List.of(event),
                List.of(link),
                true,
                3,
                2,
                6,
                SCOPE,
                Resource.create(
                        Attributes.of(AttributeKey.stringKey("service.name"), "orders"),
                        "https://schema.example/resource"));

        assertThat(exporter.export(List.of(sdkSpan)).isSuccess()).isTrue();
        var batch = input.onlyBatch();
        var resource = batch.resourceSpans().getFirst();
        var scopes = resource.scopeSpans().getFirst();
        var span = scopes.spans().getFirst();

        assertThat(resource.schemaUrl()).isEqualTo("https://schema.example/resource");
        assertThat(scopes.schemaUrl()).isEqualTo("https://schema.example/orders");
        assertThat(scopes.scope().name()).isEqualTo("orders-lib");
        assertThat(scopes.scope().version()).isEqualTo("2.1");
        assertThat(scopes.scope().attributes().getString("scope.key")).isEqualTo("scope.value");
        assertThat(span.traceId()).isEqualTo(TRACE_ID);
        assertThat(span.parentSpanId()).isEqualTo(PARENT_ID);
        assertThat(span.traceState()).isEqualTo("vendor=value");
        assertThat(span.flags()).isEqualTo(0x301L);
        assertThat(span.attributes().get("span.body")).isEqualTo(new AttributeValue.KeyValueListValue(
                Map.of("ok", new AttributeValue.BoolValue(true))));
        assertThat(span.status().code()).isEqualTo(dev.nthings.otlp4j.model.Span.Status.Code.ERROR);
        assertThat(span.droppedAttributesCount()).isEqualTo(4);
        assertThat(span.droppedEventsCount()).isEqualTo(2);
        assertThat(span.droppedLinksCount()).isEqualTo(1);
        assertThat(span.events().getFirst().droppedAttributesCount()).isEqualTo(3);
        assertThat(span.links().getFirst().droppedAttributesCount()).isEqualTo(2);
        assertThat(span.links().getFirst().flags()).isEqualTo(0x301L);
    }

    @Test
    void metricsExportPreservesPointValuesExemplarsAndUsesConfiguredTemporalitySelector() {
        var input = new RecordingInput<>(SignalDescriptor.METRICS);
        var exporter = SdkExporters.metrics(input, instrument -> AggregationTemporality.DELTA);
        var point = LongPointData.create(
                10L,
                20L,
                Attributes.of(AttributeKey.stringKey("point.key"), "point.value"),
                42L);
        var sum = SumData.createLongSumData(true, AggregationTemporality.CUMULATIVE, List.of(point));
        var metric = new TestMetricData(
                "requests",
                "request count",
                "1",
                MetricDataType.LONG_SUM,
                sum,
                Resource.empty(),
                InstrumentationScopeInfo.create("meter"));

        assertThat(exporter.getAggregationTemporality(io.opentelemetry.sdk.metrics.InstrumentType.COUNTER))
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(exporter.export(List.of(metric)).isSuccess()).isTrue();
        var mapped = input.onlyBatch().resourceMetrics().getFirst().scopeMetrics().getFirst().metrics().getFirst();
        var mappedSum = (Metric.Sum) mapped.data();
        var mappedPoint = mappedSum.points().getFirst();

        assertThat(mapped.name()).isEqualTo("requests");
        assertThat(mappedSum.temporality()).isEqualTo(Metric.AggregationTemporality.CUMULATIVE);
        assertThat(mappedSum.monotonic()).isTrue();
        assertThat(mappedPoint.value()).isEqualTo(new dev.nthings.otlp4j.model.NumberPoint.LongValue(42L));
        assertThat(mappedPoint.attributes().getString("point.key")).isEqualTo("point.value");
        assertThat(mappedPoint.startEpochNanos()).isEqualTo(10L);
        assertThat(mappedPoint.epochNanos()).isEqualTo(20L);
    }

    @Test
    void metricsExportMapsHistogramExponentialHistogramAndSummaryPoints() {
        var input = new RecordingInput<>(SignalDescriptor.METRICS);
        var exporter = SdkExporters.metrics(input);
        var exemplar = DoubleExemplarData.create(
                Attributes.of(AttributeKey.stringKey("filtered"), "yes"),
                99L,
                CONTEXT,
                1.5D);
        var histogramPoint = HistogramPointData.create(
                1L,
                2L,
                Attributes.empty(),
                3.5D,
                true,
                1.0D,
                true,
                4.0D,
                List.of(2.0D),
                List.of(1L, 2L));
        var positive = ExponentialHistogramBuckets.create(3, -2, List.of(2L, 3L));
        var negative = ExponentialHistogramBuckets.create(3, 1, List.of(4L));
        var exponentialPoint = ExponentialHistogramPointData.create(
                3,
                8.5D,
                1L,
                true,
                -1.0D,
                true,
                5.0D,
                positive,
                negative,
                3L,
                4L,
                Attributes.empty(),
                List.of(exemplar));
        var summaryPoint = SummaryPointData.create(
                5L,
                6L,
                Attributes.empty(),
                7L,
                8.0D,
                List.of(ValueAtQuantile.create(0.5D, 2.0D)));
        var doubleGaugePoint = DoublePointData.create(
                0L, 7L, Attributes.of(AttributeKey.doubleKey("gauge"), 1.25D), 3.5D, List.of());
        var doubleSumPoint = DoublePointData.create(8L, 9L, Attributes.empty(), 4.5D, List.of());
        List<MetricData> metrics = List.of(
                new TestMetricData(
                        "long-gauge", "", "", MetricDataType.LONG_GAUGE,
                        io.opentelemetry.sdk.metrics.data.GaugeData.createLongGaugeData(
                                List.of(LongPointData.create(0L, 6L, Attributes.empty(), 2L))),
                        Resource.empty(), SCOPE),
                new TestMetricData(
                        "double-gauge", "", "", MetricDataType.DOUBLE_GAUGE,
                        io.opentelemetry.sdk.metrics.data.GaugeData.createDoubleGaugeData(
                                List.of(doubleGaugePoint)),
                        Resource.empty(), SCOPE),
                new TestMetricData(
                        "double-sum", "", "", MetricDataType.DOUBLE_SUM,
                        SumData.createDoubleSumData(false, AggregationTemporality.DELTA,
                                List.of(doubleSumPoint)),
                        Resource.empty(), SCOPE),
                new TestMetricData(
                        "histogram", "", "ms", MetricDataType.HISTOGRAM,
                        HistogramData.create(AggregationTemporality.DELTA, List.of(histogramPoint)),
                        Resource.empty(), SCOPE),
                new TestMetricData(
                        "exponential", "", "", MetricDataType.EXPONENTIAL_HISTOGRAM,
                        ExponentialHistogramData.create(
                                AggregationTemporality.CUMULATIVE, List.of(exponentialPoint)),
                        Resource.empty(), SCOPE),
                new TestMetricData(
                        "summary", "", "", MetricDataType.SUMMARY,
                        SummaryData.create(List.of(summaryPoint)),
                        Resource.empty(), SCOPE));

        assertThat(exporter.export(metrics).isSuccess()).isTrue();
        var mapped = input.onlyBatch().resourceMetrics().getFirst().scopeMetrics().getFirst().metrics();
        var histogram = (Metric.Histogram) mapped.get(3).data();
        var exponential = (Metric.ExponentialHistogram) mapped.get(4).data();
        var summary = (Metric.Summary) mapped.get(5).data();
        assertThat(histogram.points().getFirst().count()).isEqualTo(3L);
        assertThat(histogram.points().getFirst().sum()).hasValue(3.5D);
        assertThat(exponential.points().getFirst().positive().offset()).isEqualTo(-2);
        assertThat(exponential.points().getFirst().negative().bucketCounts()).containsExactly(4L);
        assertThat(exponential.points().getFirst().exemplars().getFirst().traceId()).isEqualTo(TRACE_ID);
        assertThat(summary.points().getFirst().quantileValues().getFirst().quantile()).isEqualTo(0.5D);
        assertThat(mapped.get(0).data()).isInstanceOf(Metric.Gauge.class);
        assertThat(mapped.get(1).data()).isInstanceOf(Metric.Gauge.class);
        assertThat(mapped.get(2).data()).isInstanceOf(Metric.Sum.class);
        assertThat(((Metric.Sum) mapped.get(2).data()).monotonic()).isFalse();
    }

    @Test
    void logsExportPreservesStructuredBodiesAndFlushesOnShutdown() {
        var input = new RecordingInput<>(SignalDescriptor.LOGS);
        var exporter = SdkExporters.logs(input);
        var bodyValues = new LinkedHashMap<String, Value<?>>();
        bodyValues.put("answer", Value.of(42L));
        bodyValues.put("nested", Value.of(Value.of("x"), Value.of(true)));
        var sdkLog = new TestLogRecordData(
                100L,
                101L,
                CONTEXT,
                io.opentelemetry.api.logs.Severity.WARN2,
                "WARN2",
                Value.of(bodyValues),
                Attributes.of(AttributeKey.booleanKey("flag"), true),
                2,
                "log-event",
                SCOPE);

        assertThat(exporter.export(List.of(sdkLog)).isSuccess()).isTrue();
        var record = input.onlyBatch().resourceLogs().getFirst().scopeLogs().getFirst().logRecords().getFirst();
        assertThat(record.body()).isEqualTo(new AttributeValue.KeyValueListValue(Map.of(
                "answer", new AttributeValue.LongValue(42L),
                "nested", new AttributeValue.ArrayValue(List.of(
                        new AttributeValue.StringValue("x"), new AttributeValue.BoolValue(true))))));
        assertThat(record.severity()).isEqualTo(dev.nthings.otlp4j.model.LogRecord.Severity.WARN2);
        assertThat(record.droppedAttributesCount()).isEqualTo(1);
        assertThat(record.eventName()).isEqualTo("log-event");

        assertThat(exporter.shutdown().join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(input.flushCalls).isEqualTo(1);
        assertThat(exporter.export(List.of(sdkLog)).isSuccess()).isFalse();
    }

    @Test
    void shutdownWaitsForAnOutstandingExportBeforeFlushingBorrowedInput() {
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        var pending = new CompletableFuture<DeliveryResult>();
        input.nextResult = pending;
        var exporter = SdkExporters.spans(input);
        var span = new TestSpanData(
                "operation", SpanKind.INTERNAL, CONTEXT, SpanContext.getInvalid(), StatusData.unset(),
                1L, 2L, Attributes.empty(), List.of(), List.of(), true, 0, 0, 0, SCOPE,
                Resource.empty());

        var exportResult = exporter.export(List.of(span));
        var shutdownResult = exporter.shutdown();
        assertThat(shutdownResult.isDone()).isFalse();
        pending.complete(DeliveryResult.accepted(input.provenance));

        assertThat(exportResult.isSuccess()).isTrue();
        assertThat(shutdownResult.join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(input.flushCalls).isEqualTo(1);
    }

    @Test
    void shutdownTimeoutBoundsOutstandingSubmissionWithoutCancellingTheBorrowedInput() {
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        var pending = new CompletableFuture<DeliveryResult>();
        input.nextResult = pending;
        var exporter = new SdkSpanExporter(input, Duration.ofMillis(25));
        var span = new TestSpanData(
                "operation", SpanKind.INTERNAL, CONTEXT, SpanContext.getInvalid(), StatusData.unset(),
                1L, 2L, Attributes.empty(), List.of(), List.of(), true, 0, 0, 0, SCOPE,
                Resource.empty());

        var exportResult = exporter.export(List.of(span));
        var shutdownResult = exporter.shutdown();

        assertThat(shutdownResult.join(1, TimeUnit.SECONDS).isSuccess()).isFalse();
        assertThat(pending.isCancelled()).isFalse();
        assertThat(input.flushCalls).isZero();

        pending.complete(DeliveryResult.accepted(input.provenance));
        assertThat(exportResult.join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(input.flushCalls).isZero();
    }

    @Test
    void terminalDeliveryAndFlushOutcomesMapToSdkResults() {
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        var exporter = SdkExporters.spans(input);
        var span = minimalSpan();

        input.nextResult = CompletableFuture.completedFuture(
                DeliveryResult.partial(1, "one rejected", input.provenance));
        assertThat(exporter.export(List.of(span)).isSuccess()).isTrue();

        input.nextResult = CompletableFuture.completedFuture(
                DeliveryResult.permanent("rejected", input.provenance));
        assertThat(exporter.export(List.of(span)).isSuccess()).isFalse();

        input.nextResult = CompletableFuture.failedFuture(new IllegalStateException("broken"));
        assertThat(exporter.export(List.of(span)).isSuccess()).isFalse();

        input.nextFlush = CompletableFuture.completedFuture(new FlushReport(1, 0, 0, null));
        assertThat(exporter.flush().isSuccess()).isFalse();
        input.nextFlush = CompletableFuture.failedFuture(new IllegalStateException("broken flush"));
        assertThat(exporter.flush().isSuccess()).isFalse();
    }

    @Test
    void emptyAndPostShutdownExportsAreRejectedAndFactoriesValidateArguments() {
        var tracesInput = new RecordingInput<>(SignalDescriptor.TRACES);
        var spans = SdkExporters.spans(tracesInput);
        assertThat(spans.export(List.of()).isSuccess()).isTrue();
        assertThat(spans.shutdown().join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(spans.shutdown().join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(spans.export(List.of()).isSuccess()).isFalse();
        assertThat(spans.flush().isSuccess()).isFalse();

        var metrics = SdkExporters.metrics(new RecordingInput<>(SignalDescriptor.METRICS));
        assertThat(metrics.export(List.of()).isSuccess()).isTrue();
        var logs = SdkExporters.logs(new RecordingInput<>(SignalDescriptor.LOGS));
        assertThat(logs.export(List.of()).isSuccess()).isTrue();

        assertThatThrownBy(() -> SdkExporters.spans(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SdkExporters.metrics(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SdkExporters.logs(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SdkExporters.metrics(
                new RecordingInput<>(SignalDescriptor.METRICS), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SdkSpanExporter(tracesInput, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SdkSpanExporter(tracesInput, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> spans.export(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shutdownDoesNotBlockOnSynchronousBorrowedFlush() throws Exception {
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        input.blockFlush = true;
        input.flushEntered = new CountDownLatch(1);
        input.flushRelease = new CountDownLatch(1);
        var exporter = new SdkSpanExporter(input, Duration.ofMillis(40));

        var started = System.nanoTime();
        var shutdownResult = exporter.shutdown();
        var shutdownCallMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertThat(shutdownCallMillis).isLessThan(500);
        assertThat(input.flushEntered.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(shutdownResult.join(1, TimeUnit.SECONDS).isSuccess()).isFalse();
        input.flushRelease.countDown();
    }

    @Test
    void tracesMapToTheSameWireFieldsAfterTheDomainSnapshot() {
        var input = new RecordingInput<>(SignalDescriptor.TRACES);
        var exporter = SdkExporters.spans(input);
        var event = EventData.create(150L, "annotated", Attributes.empty(), 0);
        var link = LinkData.create(REMOTE_PARENT, Attributes.empty(), 0);
        var sdkSpan = new TestSpanData(
                "checkout", SpanKind.SERVER, CONTEXT, REMOTE_PARENT,
                StatusData.create(io.opentelemetry.api.trace.StatusCode.ERROR, "failed"),
                100L, 200L, Attributes.empty(), List.of(event), List.of(link), true,
                1, 1, 0, SCOPE, Resource.empty());

        assertThat(exporter.export(List.of(sdkSpan)).isSuccess()).isTrue();
        var request = SignalCodec.TRACES.toProto(input.onlyBatch());
        var span = request.getResourceSpans(0).getScopeSpans(0).getSpans(0);

        assertThat(HexFormat.of().formatHex(span.getTraceId().toByteArray())).isEqualTo(TRACE_ID);
        assertThat(HexFormat.of().formatHex(span.getParentSpanId().toByteArray())).isEqualTo(PARENT_ID);
        assertThat(span.getFlags()).isEqualTo(0x301);
        assertThat(span.getKind())
                .isEqualTo(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        assertThat(span.getStartTimeUnixNano()).isEqualTo(100L);
        assertThat(span.getEndTimeUnixNano()).isEqualTo(200L);
        assertThat(span.getEvents(0).getTimeUnixNano()).isEqualTo(150L);
        assertThat(span.getEvents(0).getName()).isEqualTo("annotated");
        assertThat(span.getLinks(0).getFlags()).isEqualTo(0x301);
        assertThat(span.getStatus().getCode())
                .isEqualTo(io.opentelemetry.proto.trace.v1.Status.StatusCode.STATUS_CODE_ERROR);
        assertThat(SignalCodec.TRACES.toDomain(request)).isEqualTo(input.onlyBatch());
    }

    @Test
    void metricsMapToTheSameWireFieldsAfterTheDomainSnapshot() {
        var input = new RecordingInput<>(SignalDescriptor.METRICS);
        var exporter = SdkExporters.metrics(input);
        var point = LongPointData.create(10L, 20L, Attributes.empty(), 42L);
        var metric = new TestMetricData(
                "requests", "request count", "1", MetricDataType.LONG_SUM,
                SumData.createLongSumData(true, AggregationTemporality.CUMULATIVE, List.of(point)),
                Resource.empty(), SCOPE);

        assertThat(exporter.export(List.of(metric)).isSuccess()).isTrue();
        var request = SignalCodec.METRICS.toProto(input.onlyBatch());
        var wireMetric = request.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
        var wireSum = wireMetric.getSum();
        var wirePoint = wireSum.getDataPoints(0);

        assertThat(wireMetric.getName()).isEqualTo("requests");
        assertThat(wireMetric.getDescription()).isEqualTo("request count");
        assertThat(wireMetric.getUnit()).isEqualTo("1");
        assertThat(wireSum.getAggregationTemporality())
                .isEqualTo(io.opentelemetry.proto.metrics.v1.AggregationTemporality
                        .AGGREGATION_TEMPORALITY_CUMULATIVE);
        assertThat(wireSum.getIsMonotonic()).isTrue();
        assertThat(wirePoint.getAsInt()).isEqualTo(42L);
        assertThat(wirePoint.getFlags()).isZero();
        assertThat(wirePoint.getStartTimeUnixNano()).isEqualTo(10L);
        assertThat(wirePoint.getTimeUnixNano()).isEqualTo(20L);
        assertThat(SignalCodec.METRICS.toDomain(request)).isEqualTo(input.onlyBatch());
    }

    @Test
    void logsMapStructuredBodiesToAnyValueOnTheWire() {
        var input = new RecordingInput<>(SignalDescriptor.LOGS);
        var exporter = SdkExporters.logs(input);
        var body = Value.of(Map.of("answer", Value.of(42L), "nested", Value.of(Value.of("x"))));
        var sdkLog = new TestLogRecordData(
                100L, 101L, CONTEXT, io.opentelemetry.api.logs.Severity.WARN2, "WARN2", body,
                Attributes.empty(), 0, "log-event", SCOPE);

        assertThat(exporter.export(List.of(sdkLog)).isSuccess()).isTrue();
        var request = SignalCodec.LOGS.toProto(input.onlyBatch());
        var record = request.getResourceLogs(0).getScopeLogs(0).getLogRecords(0);
        var bodyValue = record.getBody();

        assertThat(record.getTimeUnixNano()).isEqualTo(100L);
        assertThat(record.getObservedTimeUnixNano()).isEqualTo(101L);
        assertThat(record.getSeverityNumberValue()).isEqualTo(14);
        assertThat(record.getEventName()).isEqualTo("log-event");
        assertThat(bodyValue.hasKvlistValue()).isTrue();
        var answer = bodyValue.getKvlistValue().getValuesList().stream()
                .filter(value -> value.getKey().equals("answer"))
                .findFirst()
                .orElseThrow();
        assertThat(answer.getValue().getIntValue()).isEqualTo(42L);
        assertThat(SignalCodec.LOGS.toDomain(request)).isEqualTo(input.onlyBatch());
    }

    @Test
    void logsMapEveryPublicAnyValueShapeAndOptionalSdkField() {
        var input = new RecordingInput<>(SignalDescriptor.LOGS);
        var body = Value.of(List.of(
                Value.of("text"),
                Value.of(false),
                Value.of(42L),
                Value.of(2.5D),
                Value.of(new byte[] {1, 2}),
                Value.of(Map.of("empty", Value.empty())),
                Value.empty()));
        var attributes = Attributes.builder()
                .put(AttributeKey.booleanKey("boolean"), true)
                .put(AttributeKey.longKey("long"), 7L)
                .put(AttributeKey.doubleKey("double"), 1.5D)
                .put(AttributeKey.valueKey("bytes"), Value.of(new byte[] {3, 4}))
                .put(AttributeKey.stringArrayKey("array"), List.of("one", "two"))
                .build();
        var sdkLog = new TestLogRecordData(
                1L, 2L, null, null, null, body, attributes, attributes.size(), null,
                InstrumentationScopeInfo.create("nullable", null, null));

        var exporter = SdkExporters.logs(input);
        assertThat(exporter.export(List.of(sdkLog)).isSuccess()).isTrue();
        var record = input.onlyBatch().resourceLogs().getFirst().scopeLogs().getFirst().logRecords()
                .getFirst();
        assertThat(record.severity()).isEqualTo(dev.nthings.otlp4j.model.LogRecord.Severity.UNSPECIFIED);
        assertThat(record.severityText()).isEmpty();
        assertThat(record.eventName()).isEmpty();
        assertThat(record.traceId()).isEmpty();
        assertThat(record.body()).isInstanceOf(AttributeValue.ArrayValue.class);
        assertThat(record.attributes().get("long"))
                .isEqualTo(new AttributeValue.LongValue(7L));
        assertThat(record.attributes().get("double"))
                .isEqualTo(new AttributeValue.DoubleValue(1.5D));
        assertThat(record.attributes().get("bytes"))
                .isEqualTo(new AttributeValue.BytesValue(new byte[] {3, 4}));
        assertThat(record.attributes().get("array"))
                .isEqualTo(new AttributeValue.ArrayValue(List.of(
                        new AttributeValue.StringValue("one"),
                        new AttributeValue.StringValue("two"))));
    }

    private static final class RecordingInput<T> implements TelemetryInput<T> {
        private final SignalDescriptor<T> signal;
        private final List<T> batches = new CopyOnWriteArrayList<>();
        private final dev.nthings.otlp4j.core.result.Provenance provenance;
        private volatile CompletableFuture<DeliveryResult> nextResult;
        private volatile CompletionStage<FlushReport> nextFlush;
        private volatile int flushCalls;
        private volatile boolean blockFlush;
        private volatile CountDownLatch flushEntered;
        private volatile CountDownLatch flushRelease;

        private RecordingInput(SignalDescriptor<T> signal) {
            this.signal = signal;
            this.provenance = new dev.nthings.otlp4j.core.result.Provenance(
                    new ComponentId("test", ""), ComponentKind.EXPORTER, signal);
        }

        @Override
        public SignalDescriptor<T> signal() {
            return signal;
        }

        @Override
        public CompletionStage<DeliveryResult> submit(T data, ConsumeContext context) {
            batches.add(data);
            var result = nextResult;
            if (result == null) {
                return CompletableFuture.completedFuture(DeliveryResult.accepted(provenance));
            }
            return result;
        }

        @Override
        public CompletionStage<FlushReport> flush(Duration timeout) {
            flushCalls++;
            if (blockFlush) {
                flushEntered.countDown();
                try {
                    flushRelease.await();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.failedFuture(exception);
                }
            }
            var result = nextFlush;
            if (result != null) {
                return result;
            }
            return CompletableFuture.completedFuture(FlushReport.empty());
        }

        private T onlyBatch() {
            assertThat(batches).hasSize(1);
            return batches.getFirst();
        }
    }

    private static SpanData minimalSpan() {
        return new TestSpanData(
                "operation", SpanKind.INTERNAL, CONTEXT, SpanContext.getInvalid(), StatusData.unset(),
                1L, 2L, Attributes.empty(), List.of(), List.of(), true, 0, 0, 0, SCOPE,
                Resource.empty());
    }

    private record TestMetricData(
            String name,
            String description,
            String unit,
            MetricDataType type,
            Data<?> data,
            Resource resource,
            InstrumentationScopeInfo scope) implements MetricData {
        @Override
        public Resource getResource() {
            return resource;
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo() {
            return scope;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public String getUnit() {
            return unit;
        }

        @Override
        public MetricDataType getType() {
            return type;
        }

        @Override
        public Data<?> getData() {
            return data;
        }
    }

    private record TestSpanData(
            String name,
            SpanKind kind,
            SpanContext spanContext,
            SpanContext parentSpanContext,
            StatusData status,
            long startEpochNanos,
            long endEpochNanos,
            Attributes attributes,
            List<EventData> events,
            List<LinkData> links,
            boolean ended,
            int totalRecordedEvents,
            int totalRecordedLinks,
            int totalAttributeCount,
            InstrumentationScopeInfo scope,
            Resource resource) implements SpanData {
        @Override
        public String getName() { return name; }

        @Override
        public SpanKind getKind() { return kind; }

        @Override
        public SpanContext getSpanContext() { return spanContext; }

        @Override
        public SpanContext getParentSpanContext() { return parentSpanContext; }

        @Override
        public StatusData getStatus() { return status; }

        @Override
        public long getStartEpochNanos() { return startEpochNanos; }

        @Override
        public Attributes getAttributes() { return attributes; }

        @Override
        public List<EventData> getEvents() { return events; }

        @Override
        public List<LinkData> getLinks() { return links; }

        @Override
        public long getEndEpochNanos() { return endEpochNanos; }

        @Override
        public boolean hasEnded() { return ended; }

        @Override
        public int getTotalRecordedEvents() { return totalRecordedEvents; }

        @Override
        public int getTotalRecordedLinks() { return totalRecordedLinks; }

        @Override
        public int getTotalAttributeCount() { return totalAttributeCount; }

        @Override
        public io.opentelemetry.sdk.common.InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
            return io.opentelemetry.sdk.common.InstrumentationLibraryInfo.create(scope.getName(), scope.getVersion());
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo() { return scope; }

        @Override
        public Resource getResource() { return resource; }
    }

    private record TestLogRecordData(
            long timestamp,
            long observedTimestamp,
            SpanContext spanContext,
            io.opentelemetry.api.logs.Severity severity,
            String severityText,
            Value<?> body,
                Attributes attributes,
                int totalAttributeCount,
                String eventName,
                InstrumentationScopeInfo scope) implements LogRecordData {
        @Override
        public Resource getResource() {
            return Resource.empty();
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo() { return scope; }

        @Override
        public long getTimestampEpochNanos() { return timestamp; }

        @Override
        public long getObservedTimestampEpochNanos() { return observedTimestamp; }

        @Override
        public SpanContext getSpanContext() { return spanContext; }

        @Override
        public io.opentelemetry.api.logs.Severity getSeverity() { return severity; }

        @Override
        public String getSeverityText() { return severityText; }

        @Override
        public io.opentelemetry.sdk.logs.data.Body getBody() {
            return io.opentelemetry.sdk.logs.data.Body.string(body.asString());
        }

        @Override
        public Value<?> getBodyValue() { return body; }

        @Override
        public Attributes getAttributes() { return attributes; }

        @Override
        public int getTotalAttributeCount() { return totalAttributeCount; }

        @Override
        public String getEventName() { return eventName; }
    }
}
