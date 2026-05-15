package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Unit tests for the [Span], [LogRecord] and [Metric] builders' defaults and list semantics,
/// plus exhaustive enumeration of [LogRecord.Severity] number round-tripping.
class BuildersAndSeverityTest {

    // --- Span.builder -----------------------------------------------------------------------

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

    @Test
    void spanBuilderLinksListReplacesWhileAddLinkAppends() {
        var first = new Span.Link("t1", "s1", "", Attributes.empty(), 0, 0L);
        var second = new Span.Link("t2", "s2", "", Attributes.empty(), 0, 0L);
        var third = new Span.Link("t3", "s3", "", Attributes.empty(), 0, 0L);

        var span = Span.builder()
                .addLink(first)
                .links(List.of(second))
                .addLink(third)
                .build();

        assertThat(span.links()).containsExactly(second, third);
    }

    @Test
    void spanBuilderRetainsEverySetField() {
        var span = Span.builder()
                .traceId("abc")
                .spanId("def")
                .parentSpanId("ghi")
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

        assertThat(span.traceId()).isEqualTo("abc");
        assertThat(span.kind()).isEqualTo(Span.Kind.SERVER);
        assertThat(span.flags()).isEqualTo(7L);
        assertThat(span.endEpochNanos()).isEqualTo(200L);
        assertThat(span.status().code()).isEqualTo(Span.Status.Code.OK);
    }

    // --- LogRecord.builder ------------------------------------------------------------------

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

    @Test
    void logRecordBuilderBodyStringOverloadWrapsInAStringValue() {
        var record = LogRecord.builder().body("hello").build();

        assertThat(record.body()).isEqualTo(AttributeValue.of("hello"));
    }

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
                .traceId("trace")
                .spanId("span")
                .eventName("event")
                .build();

        assertThat(record.severity()).isEqualTo(LogRecord.Severity.ERROR);
        assertThat(record.body()).isEqualTo(AttributeValue.of(42L));
        assertThat(record.traceId()).isEqualTo("trace");
        assertThat(record.eventName()).isEqualTo("event");
    }

    // --- Metric.builder ---------------------------------------------------------------------

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

    @ParameterizedTest
    @EnumSource(LogRecord.Severity.class)
    void severityFromNumberRoundTripsEveryConstant(LogRecord.Severity severity) {
        assertThat(LogRecord.Severity.fromNumber(severity.number())).isEqualTo(severity);
    }

    @Test
    void severityFromNumberFallsBackToUnspecifiedForOutOfRangeNumbers() {
        assertThat(LogRecord.Severity.fromNumber(-1)).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(LogRecord.Severity.fromNumber(25)).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(LogRecord.Severity.fromNumber(Integer.MAX_VALUE))
                .isEqualTo(LogRecord.Severity.UNSPECIFIED);
    }

    @Test
    void severityNumbersCoverTheContiguousZeroToTwentyFourRange() {
        assertThat(LogRecord.Severity.values()).hasSize(25);
        assertThat(LogRecord.Severity.UNSPECIFIED.number()).isZero();
        assertThat(LogRecord.Severity.FATAL4.number()).isEqualTo(24);
    }
}
