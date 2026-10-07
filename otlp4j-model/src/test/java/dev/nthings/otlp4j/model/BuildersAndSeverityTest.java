package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Builder field preservation, list replacement, and malformed inputs.
@DisplayName("Builders and LogRecord.Severity")
class BuildersAndSeverityTest {

    // --- Span.builder -----------------------------------------------------------------------

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

    // --- LogRecord.builder ------------------------------------------------------------------

    @DisplayName("LogRecord.builder body(String) wraps in a StringValue")
    @Test
    void logRecordBuilderBodyStringOverloadWrapsInAStringValue() {
        var record = LogRecord.builder().body("hello").build();

        assertThat(record.body()).isEqualTo(AttributeValue.of("hello"));
    }

    @DisplayName("LogRecord rejects null required fields")
    @Test
    void logRecordRejectsNullRequiredFields() {
        assertThatThrownBy(() -> new LogRecord(
                        0, 0, null, "", AttributeValue.empty(), Attributes.empty(), 0, 0, "", "", ""))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("severity");
        assertThatThrownBy(() -> new LogRecord(
                        0, 0, LogRecord.Severity.UNSPECIFIED, null,
                        AttributeValue.empty(), Attributes.empty(), 0, 0, "", "", ""))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("severityText");
        assertThatThrownBy(() -> new LogRecord(
                        0, 0, LogRecord.Severity.UNSPECIFIED, "", null,
                        Attributes.empty(), 0, 0, "", "", ""))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("body");
        assertThatThrownBy(() -> new LogRecord(
                        0, 0, LogRecord.Severity.UNSPECIFIED, "", AttributeValue.empty(),
                        null, 0, 0, "", "", ""))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("attributes");
        assertThatThrownBy(() -> new LogRecord(
                        0, 0, LogRecord.Severity.UNSPECIFIED, "", AttributeValue.empty(),
                        Attributes.empty(), 0, 0, "", "", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("eventName");
    }

    // --- Metric.builder ---------------------------------------------------------------------

    // --- LogRecord.Severity -----------------------------------------------------------------

    @DisplayName("Severity.fromNumber falls back to UNSPECIFIED out of range")
    @Test
    void severityFromNumberFallsBackToUnspecifiedForOutOfRangeNumbers() {
        assertThat(LogRecord.Severity.fromNumber(-1)).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(LogRecord.Severity.fromNumber(25)).isEqualTo(LogRecord.Severity.UNSPECIFIED);
        assertThat(LogRecord.Severity.fromNumber(Integer.MAX_VALUE))
                .isEqualTo(LogRecord.Severity.UNSPECIFIED);
    }
}
