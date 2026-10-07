package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.junit.jupiter.api.Test;

class UnknownWireValuesTest {

    @Test
    void unknownSpanKindAndStatusDoNotDiscardTheSpan() {
        var request = ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addSpans(Span.newBuilder()
                                        .setName("checkout")
                                        .setKindValue(999)
                                        .setStatus(Status.newBuilder().setCodeValue(999).setMessage("details")))))
                .build();

        assertThat(TraceMapper.toDomain(request).spans()).singleElement().satisfies(span -> {
            assertThat(span.name()).isEqualTo("checkout");
            assertThat(span.status().message()).isEqualTo("details");
        });
    }

    @Test
    void unknownTemporalityDoesNotDiscardMetricPoints() {
        var request = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(Metric.newBuilder()
                                        .setName("requests")
                                        .setSum(Sum.newBuilder()
                                                .setAggregationTemporalityValue(999)
                                                .addDataPoints(NumberDataPoint.newBuilder().setAsInt(7))))))
                .build();

        var decoded = MetricsMapper.toDomain(request);

        assertThat(decoded.metrics()).singleElement().satisfies(metric ->
                assertThat(metric.name()).isEqualTo("requests"));
        assertThat(decoded.dataPointCount()).isEqualTo(1);
    }

    @Test
    void unknownSeverityDoesNotDiscardTheLogBody() {
        var request = ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setSeverityNumberValue(999)
                                        .setBody(AnyValue.newBuilder().setStringValue("checkout failed")))))
                .build();

        assertThat(LogsMapper.toDomain(request).logRecords()).singleElement().satisfies(record ->
                assertThat(record.body()).isEqualTo(AttributeValue.of("checkout failed")));
    }
}
