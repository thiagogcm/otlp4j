package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Exemplar;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.HistogramPoint;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import java.util.OptionalDouble;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Exemplar coverage for [MetricsMapper]: exemplars hang off number, histogram, and
/// exponential-histogram points and must survive a `toProto`/`toDomain` round-trip — both the
/// integer and double value variants, and the present/absent forms of `spanId`/`traceId`.
@DisplayName("MetricsMapper exemplar round-trips")
class ExemplarRoundTripTest {

    private static final String SPAN_ID = "00f067aa0ba902b7";
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";

    private static Exemplar longExemplar() {
        return new Exemplar(
                Attributes.builder().put("filtered", "yes").build(),
                3_000L,
                NumberPoint.longValue(42L),
                SPAN_ID,
                TRACE_ID);
    }

    private static Exemplar doubleExemplar() {
        return new Exemplar(Attributes.empty(), 4_000L, NumberPoint.doubleValue(1.5), "", "");
    }

    private static Metric gaugeWithExemplars() {
        var point = new NumberPoint(
                Attributes.builder().put("k", 1L).build(),
                1_000L,
                2_000L,
                NumberPoint.longValue(7L),
                0L,
                List.of(longExemplar(), doubleExemplar()));
        return Metric.builder()
                .name("gauge.exemplars")
                .data(new Metric.Gauge(List.of(point)))
                .build();
    }

    private static Metric histogramWithExemplar() {
        var point = new HistogramPoint(
                Attributes.empty(),
                1_000L,
                2_000L,
                10L,
                OptionalDouble.of(123.4),
                List.of(2L, 3L, 5L),
                List.of(1.0, 10.0),
                OptionalDouble.of(0.5),
                OptionalDouble.of(99.9),
                0L,
                List.of(longExemplar()));
        return Metric.builder()
                .name("histogram.exemplars")
                .data(new Metric.Histogram(
                        List.of(point), Metric.AggregationTemporality.CUMULATIVE))
                .build();
    }

    private static Metric exponentialHistogramWithExemplar() {
        var point = new ExponentialHistogramPoint(
                Attributes.empty(),
                1_000L,
                2_000L,
                7L,
                OptionalDouble.of(50.0),
                2,
                1L,
                new ExponentialHistogramPoint.Buckets(0, List.of(1L, 2L, 1L)),
                ExponentialHistogramPoint.Buckets.EMPTY,
                OptionalDouble.of(0.1),
                OptionalDouble.of(9.9),
                0.0,
                0L,
                List.of(doubleExemplar()));
        return Metric.builder()
                .name("exp.histogram.exemplars")
                .data(new Metric.ExponentialHistogram(
                        List.of(point), Metric.AggregationTemporality.DELTA))
                .build();
    }

    @DisplayName("Exemplars on every point kind round-trip losslessly")
    @Test
    void exemplarsRoundTripLosslessly() {
        var sent = Fixtures.metricsData(
                gaugeWithExemplars(), histogramWithExemplar(), exponentialHistogramWithExemplar());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent));

        assertThat(roundTripped)
                .as("exemplars must survive toDomain(toProto(x)) on every point kind")
                .isEqualTo(sent);
    }

    @DisplayName("An exemplar's value and trace/span ids survive the round-trip")
    @Test
    void exemplarValueAndIdsSurvive() {
        var sent = Fixtures.metricsData(gaugeWithExemplars());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent));

        var exemplars = firstNumberPoint(roundTripped).exemplars();
        assertThat(exemplars).hasSize(2);

        var withTrace = exemplars.getFirst();
        assertThat(withTrace.value()).isEqualTo(NumberPoint.longValue(42L));
        assertThat(withTrace.spanId()).isEqualTo(SPAN_ID);
        assertThat(withTrace.traceId()).isEqualTo(TRACE_ID);
        assertThat(withTrace.epochNanos()).isEqualTo(3_000L);
        assertThat(withTrace.filteredAttributes())
                .isEqualTo(Attributes.builder().put("filtered", "yes").build());

        var withoutTrace = exemplars.get(1);
        assertThat(withoutTrace.value()).isEqualTo(NumberPoint.doubleValue(1.5));
        assertThat(withoutTrace.spanId()).isEmpty();
        assertThat(withoutTrace.traceId()).isEmpty();
    }

    @DisplayName("A value-less (VALUE_NOT_SET) exemplar round-trips to a null value")
    @Test
    void nullValueExemplarRoundTrips() {
        var exemplar = new Exemplar(Attributes.empty(), 5_000L, null, "", "");
        var point = new NumberPoint(
                Attributes.empty(), 1_000L, 2_000L, NumberPoint.longValue(1L), 0L, List.of(exemplar));
        var sent = Fixtures.metricsData(Metric.builder()
                .name("gauge.nullexemplar")
                .data(new Metric.Gauge(List.of(point)))
                .build());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent));

        assertThat(roundTripped).isEqualTo(sent);
        assertThat(firstNumberPoint(roundTripped).exemplars().getFirst().value()).isNull();
    }

    private static NumberPoint firstNumberPoint(MetricsData data) {
        var metric = data.resourceMetrics()
                .getFirst()
                .scopeMetrics()
                .getFirst()
                .metrics()
                .getFirst();
        return ((Metric.Gauge) metric.data()).points().getFirst();
    }
}
