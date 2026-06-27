package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that `toBuilder().build()` round-trips every builder-backed record without changing
/// equality, for both fully-populated values and the nullable/empty edge cases.
@DisplayName("toBuilder round-trips")
class ToBuilderRoundTripTest {

    private static final Attributes ATTRS =
            Attributes.builder().put("k", "v").put("n", 7L).build();

    private static final Exemplar EXEMPLAR = Exemplar.builder()
            .filteredAttributes(ATTRS)
            .epochNanos(11L)
            .longValue(42L)
            .spanId("00000000000000aa")
            .traceId("000000000000000000000000000000bb")
            .build();

    @DisplayName("Span.toBuilder() round-trips a fully-populated span")
    @Test
    void spanRoundTrips() {
        var span = Span.builder()
                .traceId("0123456789abcdef0123456789abcdef")
                .spanId("0123456789abcdef")
                .parentSpanId("fedcba9876543210")
                .traceState("a=b")
                .flags(1L)
                .name("op")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(10L)
                .endEpochNanos(20L)
                .attributes(ATTRS)
                .droppedAttributesCount(2)
                .addEvent(new Span.Event(15L, "evt", ATTRS, 1))
                .droppedEventsCount(3)
                .addLink(new Span.Link("0123456789abcdef0123456789abcdef", "0123456789abcdef", "", ATTRS, 0, 0L))
                .droppedLinksCount(4)
                .status(new Span.Status(Span.Status.Code.OK, "ok"))
                .build();

        assertThat(span.toBuilder().build()).isEqualTo(span);
    }

    @DisplayName("Metric.toBuilder() round-trips each data payload, including NoData")
    @Test
    void metricRoundTrips() {
        var gauge = Metric.builder()
                .name("m")
                .description("d")
                .unit("ms")
                .data(Metric.gauge(NumberPoint.of(ATTRS, 1L, NumberPoint.longValue(5L))))
                .metadata(ATTRS)
                .build();
        assertThat(gauge.toBuilder().build()).isEqualTo(gauge);

        var noData = Metric.builder().name("empty").build();
        assertThat(noData.toBuilder().build()).isEqualTo(noData);
        assertThat(noData.toBuilder().build().hasData()).isFalse();
    }

    @DisplayName("LogRecord.toBuilder() round-trips a fully-populated record")
    @Test
    void logRecordRoundTrips() {
        var log = LogRecord.builder()
                .epochNanos(100L)
                .observedEpochNanos(101L)
                .severity(LogRecord.Severity.WARN3)
                .severityText("WARN3")
                .body(AttributeValue.of("boom"))
                .attributes(ATTRS)
                .droppedAttributesCount(1)
                .flags(1L)
                .traceId("0123456789abcdef0123456789abcdef")
                .spanId("0123456789abcdef")
                .eventName("evt")
                .build();

        assertThat(log.toBuilder().build()).isEqualTo(log);
    }

    @DisplayName("NumberPoint.toBuilder() round-trips with a value and with a null value")
    @Test
    void numberPointRoundTrips() {
        var withValue = NumberPoint.builder()
                .attributes(ATTRS)
                .startEpochNanos(1L)
                .epochNanos(2L)
                .doubleValue(1.5)
                .flags(1L)
                .addExemplar(EXEMPLAR)
                .build();
        assertThat(withValue.toBuilder().build()).isEqualTo(withValue);

        // value oneof unset → builder must not call its non-null value(...) setter.
        var nullValue = NumberPoint.builder().attributes(ATTRS).epochNanos(2L).build();
        assertThat(nullValue.value()).isNull();
        assertThat(nullValue.toBuilder().build()).isEqualTo(nullValue);
    }

    @DisplayName("HistogramPoint.toBuilder() round-trips with present and empty optionals")
    @Test
    void histogramPointRoundTrips() {
        var full = HistogramPoint.builder()
                .attributes(ATTRS)
                .startEpochNanos(1L)
                .epochNanos(2L)
                .count(10L)
                .sum(123.4)
                .bucketCounts(List.of(2L, 3L, 5L))
                .explicitBounds(List.of(1.0, 10.0))
                .min(0.5)
                .max(99.9)
                .flags(1L)
                .addExemplar(EXEMPLAR)
                .build();
        assertThat(full.toBuilder().build()).isEqualTo(full);

        // All OptionalDoubles empty and both lists empty.
        var bare = HistogramPoint.builder().count(4L).build();
        assertThat(bare.toBuilder().build()).isEqualTo(bare);
    }

    @DisplayName("ExponentialHistogramPoint.toBuilder() round-trips with present and empty optionals")
    @Test
    void exponentialHistogramPointRoundTrips() {
        var full = ExponentialHistogramPoint.builder()
                .attributes(ATTRS)
                .startEpochNanos(1L)
                .epochNanos(2L)
                .count(10L)
                .sum(50.0)
                .scale(3)
                .zeroCount(1L)
                .positive(new ExponentialHistogramPoint.Buckets(2, List.of(1L, 2L)))
                .negative(new ExponentialHistogramPoint.Buckets(1, List.of(3L)))
                .min(0.1)
                .max(9.9)
                .zeroThreshold(0.001)
                .flags(1L)
                .addExemplar(EXEMPLAR)
                .build();
        assertThat(full.toBuilder().build()).isEqualTo(full);

        var bare = ExponentialHistogramPoint.builder().count(2L).build();
        assertThat(bare.toBuilder().build()).isEqualTo(bare);
    }

    @DisplayName("SummaryPoint.toBuilder() round-trips with quantiles and bare")
    @Test
    void summaryPointRoundTrips() {
        var full = SummaryPoint.builder()
                .attributes(ATTRS)
                .startEpochNanos(1L)
                .epochNanos(2L)
                .count(10L)
                .sum(123.4)
                .addQuantileValue(new SummaryPoint.Quantile(0.5, 5.0))
                .addQuantileValue(new SummaryPoint.Quantile(0.99, 9.9))
                .flags(1L)
                .build();
        assertThat(full.toBuilder().build()).isEqualTo(full);

        var bare = SummaryPoint.builder().count(4L).build();
        assertThat(bare.toBuilder().build()).isEqualTo(bare);
    }

    @DisplayName("Exemplar.toBuilder() round-trips with a value and with a null value")
    @Test
    void exemplarRoundTrips() {
        assertThat(EXEMPLAR.toBuilder().build()).isEqualTo(EXEMPLAR);

        var nullValue = Exemplar.of(ATTRS, 5L, null);
        assertThat(nullValue.value()).isNull();
        assertThat(nullValue.toBuilder().build()).isEqualTo(nullValue);
    }
}
