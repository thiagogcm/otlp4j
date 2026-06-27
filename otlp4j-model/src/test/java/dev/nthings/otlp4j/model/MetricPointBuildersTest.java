package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.OptionalDouble;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Metric point builders and factories")
class MetricPointBuildersTest {

    @DisplayName("NumberPoint.builder() applies empty/zero defaults")
    @Test
    void numberPointDefaults() {
        var point = NumberPoint.builder().build();

        assertThat(point.attributes()).isEqualTo(Attributes.empty());
        assertThat(point.startEpochNanos()).isZero();
        assertThat(point.epochNanos()).isZero();
        assertThat(point.value()).isNull();
        assertThat(point.flags()).isZero();
        assertThat(point.exemplars()).isEmpty();
    }

    @DisplayName("NumberPoint value/exemplar setters and of() factories")
    @Test
    void numberPointSetters() {
        var exemplar = Exemplar.builder().longValue(7L).build();
        var built = NumberPoint.builder()
                .attributes(Attributes.builder().put("k", "v").build())
                .startEpochNanos(1L)
                .epochNanos(2L)
                .longValue(42L)
                .flags(3L)
                .addExemplar(exemplar)
                .build();

        assertThat(built.value()).isEqualTo(new NumberPoint.LongValue(42L));
        assertThat(built.exemplars()).containsExactly(exemplar);
        assertThat(built.flags()).isEqualTo(3L);

        var factory = NumberPoint.of(Attributes.empty(), 9L, NumberPoint.doubleValue(1.5));
        assertThat(factory.epochNanos()).isEqualTo(9L);
        assertThat(factory.startEpochNanos()).isZero();
        assertThat(factory.value()).isEqualTo(new NumberPoint.DoubleValue(1.5));
        assertThat(factory.exemplars()).isEmpty();
    }

    @DisplayName("HistogramPoint.builder() defaults to empty Optionals and lists")
    @Test
    void histogramPointDefaults() {
        var point = HistogramPoint.builder().build();

        assertThat(point.sum()).isEmpty();
        assertThat(point.min()).isEmpty();
        assertThat(point.max()).isEmpty();
        assertThat(point.bucketCounts()).isEmpty();
        assertThat(point.explicitBounds()).isEmpty();
        assertThat(point.exemplars()).isEmpty();
    }

    @DisplayName("HistogramPoint ergonomic double setters wrap into OptionalDouble")
    @Test
    void histogramPointOptionalSetters() {
        var point = HistogramPoint.builder()
                .count(10L)
                .sum(123.4)
                .min(0.5)
                .max(99.9)
                .bucketCounts(List.of(2L, 3L, 5L))
                .explicitBounds(List.of(1.0, 10.0))
                .build();

        assertThat(point.sum()).hasValue(123.4);
        assertThat(point.min()).hasValue(0.5);
        assertThat(point.max()).hasValue(99.9);
        assertThat(point.bucketCounts()).containsExactly(2L, 3L, 5L);
        assertThat(point.explicitBounds()).containsExactly(1.0, 10.0);
    }

    @DisplayName("HistogramPoint accepts a valid off-by-one and a both-empty point")
    @Test
    void histogramInvariantAccepted() {
        assertThat(HistogramPoint.builder()
                        .addBucketCount(1L)
                        .addBucketCount(2L)
                        .addBucketCount(3L)
                        .addExplicitBound(0.5)
                        .addExplicitBound(5.0)
                        .build()
                        .bucketCounts())
                .hasSize(3);

        // A count/sum-only point leaves both lists empty.
        assertThat(HistogramPoint.builder().count(4L).sum(8.0).build().bucketCounts()).isEmpty();
    }

    @DisplayName("HistogramPoint rejects a mismatched bucket/bound pair from builder and constructor")
    @Test
    void histogramInvariantViolated() {
        // 2 buckets with 2 bounds breaks the "buckets == bounds + 1" rule.
        assertThatThrownBy(() -> HistogramPoint.builder()
                        .addBucketCount(1L)
                        .addBucketCount(2L)
                        .addExplicitBound(0.5)
                        .addExplicitBound(5.0)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2")
                .hasMessageContaining("1");

        assertThatThrownBy(() -> new HistogramPoint(
                        Attributes.empty(),
                        0L,
                        0L,
                        0L,
                        OptionalDouble.empty(),
                        List.of(1L, 2L),
                        List.of(0.5, 5.0),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        0L,
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("ExponentialHistogramPoint.builder() defaults buckets to EMPTY")
    @Test
    void exponentialPointDefaults() {
        var point = ExponentialHistogramPoint.builder().build();

        assertThat(point.positive()).isEqualTo(ExponentialHistogramPoint.Buckets.EMPTY);
        assertThat(point.negative()).isEqualTo(ExponentialHistogramPoint.Buckets.EMPTY);
        assertThat(point.sum()).isEmpty();
        assertThat(point.exemplars()).isEmpty();
    }

    @DisplayName("ExponentialHistogramPoint sets the positive side and defaults the negative to EMPTY")
    @Test
    void exponentialPointBuckets() {
        var point = ExponentialHistogramPoint.builder()
                .scale(2)
                .zeroCount(1L)
                .positive(new ExponentialHistogramPoint.Buckets(3, List.of(1L, 2L)))
                .build();

        assertThat(point.positive()).isEqualTo(new ExponentialHistogramPoint.Buckets(3, List.of(1L, 2L)));
        assertThat(point.negative()).isEqualTo(ExponentialHistogramPoint.Buckets.EMPTY);
    }

    @DisplayName("SummaryPoint.builder() applies empty/zero defaults")
    @Test
    void summaryPointDefaults() {
        var point = SummaryPoint.builder().build();

        assertThat(point.attributes()).isEqualTo(Attributes.empty());
        assertThat(point.startEpochNanos()).isZero();
        assertThat(point.epochNanos()).isZero();
        assertThat(point.count()).isZero();
        assertThat(point.sum()).isZero();
        assertThat(point.quantileValues()).isEmpty();
        assertThat(point.flags()).isZero();
    }

    @DisplayName("SummaryPoint quantile setters and of() factory")
    @Test
    void summaryPointSetters() {
        var quantile = new SummaryPoint.Quantile(0.99, 12.3);
        var built = SummaryPoint.builder()
                .attributes(Attributes.builder().put("k", "v").build())
                .startEpochNanos(1L)
                .epochNanos(2L)
                .count(10L)
                .sum(123.4)
                .addQuantileValue(quantile)
                .flags(3L)
                .build();

        assertThat(built.count()).isEqualTo(10L);
        assertThat(built.sum()).isEqualTo(123.4);
        assertThat(built.quantileValues()).containsExactly(quantile);
        assertThat(built.flags()).isEqualTo(3L);

        var factory = SummaryPoint.of(Attributes.empty(), 0L, 9L, 2L, 3.0, List.of(quantile));
        assertThat(factory.epochNanos()).isEqualTo(9L);
        assertThat(factory.startEpochNanos()).isZero();
        assertThat(factory.flags()).isZero();
        assertThat(factory.quantileValues()).containsExactly(quantile);
    }

    @DisplayName("Exemplar.builder() defaults ids to empty and value to null")
    @Test
    void exemplarDefaults() {
        var exemplar = Exemplar.builder().build();

        assertThat(exemplar.spanId()).isEmpty();
        assertThat(exemplar.traceId()).isEmpty();
        assertThat(exemplar.value()).isNull();
        assertThat(exemplar.filteredAttributes()).isEqualTo(Attributes.empty());
    }

    @DisplayName("Exemplar.of() leaves trace/span ids empty")
    @Test
    void exemplarFactory() {
        var exemplar = Exemplar.of(Attributes.empty(), 5L, NumberPoint.longValue(1L));

        assertThat(exemplar.epochNanos()).isEqualTo(5L);
        assertThat(exemplar.spanId()).isEmpty();
        assertThat(exemplar.traceId()).isEmpty();
    }

    @DisplayName("Metric data factories produce the matching sealed subtype")
    @Test
    void metricDataFactories() {
        var np = NumberPoint.of(Attributes.empty(), 1L, NumberPoint.longValue(1L));

        assertThat(Metric.gauge(np).points()).containsExactly(np);

        var sum = Metric.sum(Metric.AggregationTemporality.CUMULATIVE, true, np);
        assertThat(sum.temporality()).isEqualTo(Metric.AggregationTemporality.CUMULATIVE);
        assertThat(sum.monotonic()).isTrue();
        assertThat(sum.points()).containsExactly(np);

        var hp = HistogramPoint.builder().count(1L).build();
        assertThat(Metric.histogram(Metric.AggregationTemporality.DELTA, hp).points()).containsExactly(hp);

        var ep = ExponentialHistogramPoint.builder().build();
        assertThat(Metric.exponentialHistogram(Metric.AggregationTemporality.DELTA, ep).points())
                .containsExactly(ep);

        var sp = SummaryPoint.of(Attributes.empty(), 0L, 1L, 2L, 3.0, List.of());
        assertThat(Metric.summary(sp).points()).containsExactly(sp);
    }
}
