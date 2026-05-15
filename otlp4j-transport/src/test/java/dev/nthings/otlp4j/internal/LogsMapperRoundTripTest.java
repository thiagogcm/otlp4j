package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Mapper-unit round-trip coverage for [LogsMapper]: log records with different body variants
/// and trace/span ids present and empty.
class LogsMapperRoundTripTest {

    static Stream<LogRecord> logRecordVariants() {
        return Stream.of(
                // string body, ids present
                LogRecord.builder()
                        .epochNanos(1_700_000_000_000_000_000L)
                        .observedEpochNanos(1_700_000_000_000_000_001L)
                        .severity(LogRecord.Severity.ERROR)
                        .severityText("ERROR")
                        .body("a string body")
                        .attributes(Attributes.builder().put("k", 1L).build())
                        .droppedAttributesCount(2)
                        .flags(1L)
                        .traceId("0102030405060708090a0b0c0d0e0f10")
                        .spanId("0102030405060708")
                        .eventName("evt")
                        .build(),
                // structured map body, ids empty
                LogRecord.builder()
                        .epochNanos(1L)
                        .severity(LogRecord.Severity.INFO)
                        .body(AttributeValue.of(Map.of(
                                "nested", AttributeValue.of(List.of(AttributeValue.of(true))))))
                        .build(),
                // empty body
                LogRecord.builder()
                        .severity(LogRecord.Severity.UNSPECIFIED)
                        .body(AttributeValue.empty())
                        .build(),
                // long body
                LogRecord.builder()
                        .severity(LogRecord.Severity.DEBUG)
                        .body(AttributeValue.of(99L))
                        .build());
    }

    @ParameterizedTest
    @MethodSource("logRecordVariants")
    void roundTripsLogRecordVariants(LogRecord record) {
        var sent = Fixtures.logsData(record);
        assertThat(LogsMapper.toDomain(LogsMapper.toProto(sent)))
                .as("toDomain(toProto(x)) must equal x for every log-record body / id variant")
                .isEqualTo(sent);
    }

    @Test
    void roundTripsAnEmptyLogsData() {
        var sent = new LogsData(List.of());
        assertThat(LogsMapper.toDomain(LogsMapper.toProto(sent))).isEqualTo(sent);
    }
}
