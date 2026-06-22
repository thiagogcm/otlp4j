package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Unit tests for the [Span], [LogRecord] and [Metric] builders' defaults and list semantics,
/// plus exhaustive enumeration of [LogRecord.Severity] number round-tripping.
@DisplayName("Builders and LogRecord.Severity")
class BuildersAndSeverityTest {

    // --- Span.builder -----------------------------------------------------------------------

    @DisplayName("Span.builder defaults every field to empty or zero")
    @Test
    void spanBuilderDefaultsEveryFieldToItsEmptyOrZeroValue() {
        var span = Span.builder().build();

        assertThat(span.traceId()).isEmpty();
        assertThat(span.spanId()).isEmpty();
        assertThat(span.parentSpanId()).isEmpty();
        assertThat(span.traceState()).isEmpty();
        assertThat(span.flags()).isZero();
        assertThat(span.name()).isEmpty();
        assertThat(span.kind()).isEqualTo(Span.Kind.UNSPECIFIED);
        assertThat(span.startEpochNanos()).isZero();
        assertThat(span.endEpochNanos()).isZero();
        assertThat(span.attributes()).isEqualTo(Attributes.empty());
        assertThat(span.droppedAttributesCount()).isZero();
        assertThat(span.events()).isEmpty();
        assertThat(span.droppedEventsCount()).isZero();
        assertThat(span.links()).isEmpty();
        assertThat(span.droppedLinksCount()).isZero();
        assertThat(span.status()).isEqualTo(Span.Status.UNSET);
    }

    @DisplayName("Span.builder events(List) replaces while addEvent appends")
    @Test
    void spanBuilderEventsListReplacesWhileAddEventAppends() {
        var first = new Span.Event(1L, "first", Attributes.empty(), 0);
        var second = new Span.Event(2L, "second", Attributes.empty(), 0);
        var third = new Span.Event(3L, "third", Attributes.empty(), 0);

        var span = Span.builder()
                .addEvent(first)
                .events(List.of(second))
                .addEvent(third)
                .build();

        assertThat(span.events())
                .as("events(List) replaces the accumulated events, addEvent appends")
                .containsExactly(second, third);
    }

    @DisplayName("Span.builder links(List) replaces while addLink appends")
    @Test
    void spanBuilderLinksListReplacesWhileAddLinkAppends() {
        var first = new Span.Link(
                "0102030405060708090a0b0c0d0e0f10", "0102030405060708", "", Attributes.empty(), 0, 0L);
        var second = new Span.Link(
                "1112131415161718191a1b1c1d1e1f20", "1112131415161718", "", Attributes.empty(), 0, 0L);
        var third = new Span.Link(
                "2122232425262728292a2b2c2d2e2f30", "2122232425262728", "", Attributes.empty(), 0, 0L);

        var span = Span.builder()
                .addLink(first)
                .links(List.of(second))
                .addLink(third)
                .build();

        assertThat(span.links()).containsExactly(second, third);
    }

    @DisplayName("Span.builder retains every set field")
    @Test
    void spanBuilderRetainsEverySetField() {
        var span = Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .parentSpanId("1112131415161718")
                .traceState("k=v")
                .flags(7L)
                .name("op")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(100L)
                .endEpochNanos(200L)
                .droppedAttributesCount(1)
                .droppedEventsCount(2)
                .droppedLinksCount(3)
                .status(new Span.Status(Span.Status.Code.OK, "fine"))
                .build();

        assertThat(span.traceId()).isEqualTo("0102030405060708090a0b0c0d0e0f10");
        assertThat(span.kind()).isEqualTo(Span.Kind.SERVER);
        assertThat(span.flags()).isEqualTo(7L);
        assertThat(span.endEpochNanos()).isEqualTo(200L);
        assertThat(span.status().code()).isEqualTo(Span.Status.Code.OK);
    }

    // --- LogRecord.builder ------------------------------------------------------------------

    @DisplayName("LogRecord.builder defaults every field to empty or zero")
    @Test
    void logRecordBuilderDefaultsEveryFieldToItsEmptyOrZeroValue() {
        var record = LogRecord.builder().build();

        assertThat(record.epochNanos()).isZero();
        assertThat(record.observedEpochNanos()).isZero();
        assertThat(record.severity()).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(record.severityText()).isEmpty();
        assertThat(record.body()).isEqualTo(AttributeValue.empty());
        assertThat(record.attributes()).isEqualTo(Attributes.empty());
        assertThat(record.droppedAttributesCount()).isZero();
        assertThat(record.flags()).isZero();
        assertThat(record.traceId()).isEmpty();
        assertThat(record.spanId()).isEmpty();
        assertThat(record.eventName()).isEmpty();
    }

    @DisplayName("LogRecord.builder body(String) wraps in a StringValue")
    @Test
    void logRecordBuilderBodyStringOverloadWrapsInAStringValue() {
        var record = LogRecord.builder().body("hello").build();

        assertThat(record.body()).isEqualTo(AttributeValue.of("hello"));
    }

    @DisplayName("LogRecord.builder retains every set field")
    @Test
    void logRecordBuilderRetainsEverySetField() {
        var record = LogRecord.builder()
                .epochNanos(1L)
                .observedEpochNanos(2L)
                .severity(LogRecord.Severity.ERROR)
                .severityText("ERROR")
                .body(AttributeValue.of(42L))
                .droppedAttributesCount(3)
                .flags(4L)
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .eventName("event")
                .build();

        assertThat(record.severity()).isEqualTo(LogRecord.Severity.ERROR);
        assertThat(record.body()).isEqualTo(AttributeValue.of(42L));
        assertThat(record.traceId()).isEqualTo("0102030405060708090a0b0c0d0e0f10");
        assertThat(record.eventName()).isEqualTo("event");
    }

    // --- Metric.builder ---------------------------------------------------------------------

    @DisplayName("Metric.builder defaults text fields empty and metadata to empty Attributes")
    @Test
    void metricBuilderDefaultsTextFieldsToEmptyAndMetadataToEmptyAttributes() {
        var data = new Metric.Gauge(List.of());
        var metric = Metric.builder().data(data).build();

        assertThat(metric.name()).isEmpty();
        assertThat(metric.description()).isEmpty();
        assertThat(metric.unit()).isEmpty();
        assertThat(metric.metadata()).isEqualTo(Attributes.empty());
        assertThat(metric.data()).isSameAs(data);
    }

    @DisplayName("Metric.builder retains every set field")
    @Test
    void metricBuilderRetainsEverySetField() {
        var metadata = Attributes.builder().put("k", "v").build();
        var metric = Metric.builder()
                .name("requests")
                .description("count of requests")
                .unit("1")
                .data(new Metric.Summary(List.of()))
                .metadata(metadata)
                .build();

        assertThat(metric.name()).isEqualTo("requests");
        assertThat(metric.description()).isEqualTo("count of requests");
        assertThat(metric.unit()).isEqualTo("1");
        assertThat(metric.metadata()).isEqualTo(metadata);
        assertThat(metric.data()).isInstanceOf(Metric.Summary.class);
    }

    // --- LogRecord.Severity -----------------------------------------------------------------

    @DisplayName("Severity.fromNumber round-trips every constant")
    @ParameterizedTest
    @EnumSource(LogRecord.Severity.class)
    void severityFromNumberRoundTripsEveryConstant(LogRecord.Severity severity) {
        assertThat(LogRecord.Severity.fromNumber(severity.number())).isEqualTo(severity);
    }

    @DisplayName("Severity.fromNumber falls back to UNSPECIFIED out of range")
    @Test
    void severityFromNumberFallsBackToUnspecifiedForOutOfRangeNumbers() {
        assertThat(LogRecord.Severity.fromNumber(-1)).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(LogRecord.Severity.fromNumber(25)).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(LogRecord.Severity.fromNumber(Integer.MAX_VALUE))
                .isEqualTo(LogRecord.Severity.UNSPECIFIED);
    }

    @DisplayName("Severity numbers cover the contiguous 0 to 24 range")
    @Test
    void severityNumbersCoverTheContiguousZeroToTwentyFourRange() {
        assertThat(LogRecord.Severity.values()).hasSize(25);
        assertThat(LogRecord.Severity.UNSPECIFIED.number()).isZero();
        assertThat(LogRecord.Severity.FATAL4.number()).isEqualTo(24);
    }
}
