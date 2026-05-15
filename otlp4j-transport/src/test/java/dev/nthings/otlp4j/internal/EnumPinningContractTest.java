package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Pins the number mapping between domain enums and generated proto enums.
///
/// Every mapped constant round-trips so enum reordering or insertion fails here.
class EnumPinningContractTest {

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
}
