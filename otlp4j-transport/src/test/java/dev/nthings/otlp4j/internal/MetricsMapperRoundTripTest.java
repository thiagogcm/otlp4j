package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.HistogramPoint;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Mapper-unit round-trip coverage for [MetricsMapper]: one case per `Metric.Data` kind,
/// both `NumberPoint` value variants, and the present/absent forms of the histogram optional
/// fields (`hasSum`/`hasMin`/`hasMax`).
@DisplayName("MetricsMapper round-trips")
class MetricsMapperRoundTripTest {

    private static NumberPoint longPoint() {
        return new NumberPoint(
                Attributes.builder().put("k", 1L).build(),
                1_000L,
                2_000L,
                NumberPoint.longValue(125L),
                0L,
                List.of());
    }

    private static NumberPoint doublePoint() {
        return new NumberPoint(
                Attributes.empty(), 1_000L, 2_000L, NumberPoint.doubleValue(0.73), 0L, List.of());
    }

    private static HistogramPoint histogramPoint(boolean optionalsPresent) {
        return new HistogramPoint(
                Attributes.empty(),
                1_000L,
                2_000L,
                10L,
                optionalsPresent ? OptionalDouble.of(123.4) : OptionalDouble.empty(),
                List.of(2L, 3L, 5L),
                List.of(1.0, 10.0),
                optionalsPresent ? OptionalDouble.of(0.5) : OptionalDouble.empty(),
                optionalsPresent ? OptionalDouble.of(99.9) : OptionalDouble.empty(),
                0L,
                List.of());
    }

    private static ExponentialHistogramPoint exponentialPoint(boolean optionalsPresent) {
        return new ExponentialHistogramPoint(
                Attributes.empty(),
                1_000L,
                2_000L,
                7L,
                optionalsPresent ? OptionalDouble.of(50.0) : OptionalDouble.empty(),
                2,
                1L,
                new ExponentialHistogramPoint.Buckets(0, List.of(1L, 2L, 1L)),
                ExponentialHistogramPoint.Buckets.EMPTY,
                optionalsPresent ? OptionalDouble.of(0.1) : OptionalDouble.empty(),
                optionalsPresent ? OptionalDouble.of(9.9) : OptionalDouble.empty(),
                0.0,
                0L,
                List.of());
    }

    static Stream<Metric> everyMetricDataKind() {
        return Stream.of(
                Metric.builder()
                        .name("gauge.long")
                        .data(new Metric.Gauge(List.of(longPoint())))
                        .build(),
                Metric.builder()
                        .name("gauge.double")
                        .data(new Metric.Gauge(List.of(doublePoint())))
                        .build(),
                Metric.builder()
                        .name("sum")
                        .data(new Metric.Sum(
                                List.of(longPoint(), doublePoint()),
                                Metric.AggregationTemporality.CUMULATIVE,
                                true))
                        .build(),
                Metric.builder()
                        .name("histogram.optionals-present")
                        .data(new Metric.Histogram(
                                List.of(histogramPoint(true)),
                                Metric.AggregationTemporality.CUMULATIVE))
                        .build(),
                Metric.builder()
                        .name("histogram.optionals-absent")
                        .data(new Metric.Histogram(
                                List.of(histogramPoint(false)),
                                Metric.AggregationTemporality.DELTA))
                        .build(),
                Metric.builder()
                        .name("exp.histogram.optionals-present")
                        .data(new Metric.ExponentialHistogram(
                                List.of(exponentialPoint(true)),
                                Metric.AggregationTemporality.CUMULATIVE))
                        .build(),
                Metric.builder()
                        .name("exp.histogram.optionals-absent")
                        .data(new Metric.ExponentialHistogram(
                                List.of(exponentialPoint(false)),
                                Metric.AggregationTemporality.DELTA))
                        .build(),
                Metric.builder()
                        .name("summary")
                        .data(new Metric.Summary(List.of(new SummaryPoint(
                                Attributes.empty(),
                                1_000L,
                                2_000L,
                                100L,
                                543.2,
                                List.of(
                                        new SummaryPoint.Quantile(0.5, 10.0),
                                        new SummaryPoint.Quantile(0.99, 99.0)),
                                0L))))
                        .build());
    }

    @DisplayName("Every Metric.Data kind round-trips through MetricsMapper")
    @ParameterizedTest
    @MethodSource("everyMetricDataKind")
    void roundTripsEveryMetricDataKind(Metric metric) {
        var sent = Fixtures.metricsData(metric);
        assertThat(MetricsMapper.toDomain(MetricsMapper.toProto(sent)))
                .as("toDomain(toProto(x)) must equal x for every Metric.Data kind")
                .isEqualTo(sent);
    }

    @DisplayName("Empty MetricsData round-trips unchanged")
    @Test
    void roundTripsAnEmptyMetricsData() {
        var sent = Fixtures.metricsData();
        assertThat(MetricsMapper.toDomain(MetricsMapper.toProto(sent))).isEqualTo(sent);
    }

    @DisplayName("NumberPoint with unset value oneof round-trips as null")
    @Test
    void roundTripsANumberPointWithNoValue() {
        var point = new NumberPoint(Attributes.empty(), 1_000L, 2_000L, null, 0L, List.of());
        var sent = Fixtures.metricsData(Metric.builder()
                .name("gauge.no-value")
                .data(new Metric.Gauge(List.of(point)))
                .build());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent));

        assertThat(roundTripped)
                .as("a NumberPoint with an unset value oneof must round-trip with a null value")
                .isEqualTo(sent);
    }
}
