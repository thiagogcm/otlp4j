package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Status;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Pins the number mapping between domain enums and generated proto enums.
///
/// Every mapped constant round-trips so enum reordering or insertion fails here.
@DisplayName("Enum number-mapping contract")
class EnumPinningContractTest {

    @DisplayName("Every Span.Kind round-trips through TraceMapper")
    @ParameterizedTest
    @EnumSource(Span.Kind.class)
    void everySpanKindRoundTripsThroughTheTraceMapper(Span.Kind kind) {
        var sent = Fixtures.traceData(Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("op")
                .kind(kind)
                .build());

        var roundTripped =
                TraceMapper.toDomain(TraceMapper.toProto(sent)).spans().getFirst();

        assertThat(roundTripped.kind())
                .as("Span.Kind %s must survive ordinal<->forNumber mapping", kind)
                .isEqualTo(kind);
    }

    @DisplayName("Every Span.Status.Code round-trips through TraceMapper")
    @ParameterizedTest
    @EnumSource(Span.Status.Code.class)
    void everyStatusCodeRoundTripsThroughTheTraceMapper(Span.Status.Code code) {
        var sent = Fixtures.traceData(Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("op")
                .kind(Span.Kind.INTERNAL)
                .status(new Span.Status(code, "msg"))
                .build());

        var roundTripped =
                TraceMapper.toDomain(TraceMapper.toProto(sent)).spans().getFirst();

        assertThat(roundTripped.status().code())
                .as("Span.Status.Code %s must survive ordinal<->forNumber mapping", code)
                .isEqualTo(code);
    }

    @DisplayName("Every Metric.AggregationTemporality round-trips through MetricsMapper")
    @ParameterizedTest
    @EnumSource(Metric.AggregationTemporality.class)
    void everyAggregationTemporalityRoundTripsThroughTheMetricsMapper(
            Metric.AggregationTemporality temporality) {
        var point = new NumberPoint(
                Attributes.empty(), 0L, 1L, NumberPoint.longValue(1L), 0L);
        var sent = Fixtures.metricsData(Metric.builder()
                .name("sum")
                .data(new Metric.Sum(List.of(point), temporality, true))
                .build());

        var roundTripped = (Metric.Sum) MetricsMapper.toDomain(MetricsMapper.toProto(sent))
                .metrics()
                .getFirst()
                .data();

        assertThat(roundTripped.temporality())
                .as("AggregationTemporality %s must survive ordinal<->forNumber mapping", temporality)
                .isEqualTo(temporality);
    }

    @DisplayName("Every LogRecord.Severity round-trips through LogsMapper")
    @ParameterizedTest
    @EnumSource(LogRecord.Severity.class)
    void everyLogSeverityRoundTripsThroughTheLogsMapper(LogRecord.Severity severity) {
        var sent = Fixtures.logsData(LogRecord.builder()
                .severity(severity)
                .body("body")
                .build());

        var roundTripped =
                LogsMapper.toDomain(LogsMapper.toProto(sent)).logRecords().getFirst();

        assertThat(roundTripped.severity())
                .as("LogRecord.Severity %s (number %d) must survive .number()<->forNumber mapping",
                        severity, severity.number())
                .isEqualTo(severity);
    }

    // --- forward-compatibility: unknown wire numbers degrade instead of rejecting the batch -----
    // Unknown numbers decode to UNRECOGNIZED; the raw *Value() accessors degrade them to the
    // UNSPECIFIED/UNSET fallback instead of throwing and rejecting the whole batch.

    private static final int UNKNOWN_WIRE_NUMBER = 999;

    @DisplayName("An unknown Span.Kind/Status.Code number degrades without throwing")
    @Test
    void unknownSpanEnumsDegradeWithoutThrowing() {
        // Proto Span clashes with the imported domain Span, so only that type stays fully qualified.
        var request = ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder()
                                        .setName("op")
                                        .setKindValue(UNKNOWN_WIRE_NUMBER)
                                        .setStatus(Status.newBuilder()
                                                .setCodeValue(UNKNOWN_WIRE_NUMBER)))))
                .build();

        var span = TraceMapper.toDomain(request).spans().getFirst();

        assertThat(span.kind()).isEqualTo(Span.Kind.UNSPECIFIED);
        assertThat(span.status().code()).isEqualTo(Span.Status.Code.UNSET);
    }

    @DisplayName("An unknown AggregationTemporality number degrades without throwing")
    @Test
    void unknownAggregationTemporalityDegradesWithoutThrowing() {
        // Proto Metric clashes with the imported domain Metric, so only that type stays qualified.
        var request = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                                        .setName("sum")
                                        .setSum(Sum.newBuilder()
                                                .setAggregationTemporalityValue(UNKNOWN_WIRE_NUMBER)
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setAsInt(1))))))
                .build();

        var sum = (Metric.Sum) MetricsMapper.toDomain(request).metrics().getFirst().data();

        assertThat(sum.temporality()).isEqualTo(Metric.AggregationTemporality.UNSPECIFIED);
    }

    @DisplayName("An unknown LogRecord.Severity number degrades without throwing")
    @Test
    void unknownLogSeverityDegradesWithoutThrowing() {
        // Proto LogRecord clashes with the imported domain LogRecord, so only it stays qualified.
        var request = ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(io.opentelemetry.proto.logs.v1.LogRecord.newBuilder()
                                        .setSeverityNumberValue(UNKNOWN_WIRE_NUMBER))))
                .build();

        var record = LogsMapper.toDomain(request).logRecords().getFirst();

        assertThat(record.severity()).isEqualTo(LogRecord.Severity.UNSPECIFIED);
    }
}
