package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.HistogramPoint;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/// Maps metrics between generated proto types and [MetricsData].
///
/// Exemplars are not surfaced in the domain model and are dropped in both directions.
final class MetricsMapper {

    private static final Metric.AggregationTemporality[] TEMPORALITIES =
            Metric.AggregationTemporality.values();

    private MetricsMapper() {}

    // --- proto -> domain ---------------------------------------------------------------------

    public static MetricsData toDomain(ExportMetricsServiceRequest request) {
        var resourceMetrics =
                new ArrayList<MetricsData.ResourceMetrics>(request.getResourceMetricsCount());
        for (var rm : request.getResourceMetricsList()) {
            resourceMetrics.add(toDomain(rm));
        }
        return new MetricsData(resourceMetrics);
    }

    private static MetricsData.ResourceMetrics toDomain(
            io.opentelemetry.proto.metrics.v1.ResourceMetrics rm) {
        var scopeMetrics = new ArrayList<MetricsData.ScopeMetrics>(rm.getScopeMetricsCount());
        for (var sm : rm.getScopeMetricsList()) {
            scopeMetrics.add(toDomain(sm));
        }
        return new MetricsData.ResourceMetrics(
                CommonMapper.resource(rm.getResource()), rm.getSchemaUrl(), scopeMetrics);
    }

    private static MetricsData.ScopeMetrics toDomain(
            io.opentelemetry.proto.metrics.v1.ScopeMetrics sm) {
        var metrics = new ArrayList<Metric>(sm.getMetricsCount());
        for (var metric : sm.getMetricsList()) {
            metrics.add(toDomain(metric));
        }
        return new MetricsData.ScopeMetrics(
                CommonMapper.scope(sm.getScope()), sm.getSchemaUrl(), metrics);
    }

    private static Metric toDomain(io.opentelemetry.proto.metrics.v1.Metric metric) {
        return new Metric(
                metric.getName(),
                metric.getDescription(),
                metric.getUnit(),
                dataToDomain(metric),
                CommonMapper.attributes(metric.getMetadataList()));
    }

    private static Metric.Data dataToDomain(io.opentelemetry.proto.metrics.v1.Metric metric) {
        return switch (metric.getDataCase()) {
            case GAUGE -> new Metric.Gauge(numberPointsToDomain(metric.getGauge().getDataPointsList()));
            case SUM -> {
                var sum = metric.getSum();
                yield new Metric.Sum(
                        numberPointsToDomain(sum.getDataPointsList()),
                        temporality(sum.getAggregationTemporality().getNumber()),
                        sum.getIsMonotonic());
            }
            case HISTOGRAM -> {
                var histogram = metric.getHistogram();
                yield new Metric.Histogram(
                        histogramPointsToDomain(histogram.getDataPointsList()),
                        temporality(histogram.getAggregationTemporality().getNumber()));
            }
            case EXPONENTIAL_HISTOGRAM -> {
                var histogram = metric.getExponentialHistogram();
                yield new Metric.ExponentialHistogram(
                        exponentialHistogramPointsToDomain(histogram.getDataPointsList()),
                        temporality(histogram.getAggregationTemporality().getNumber()));
            }
            case SUMMARY ->
                new Metric.Summary(summaryPointsToDomain(metric.getSummary().getDataPointsList()));
            case DATA_NOT_SET -> null;
        };
    }

    private static List<NumberPoint> numberPointsToDomain(List<NumberDataPoint> points) {
        var result = new ArrayList<NumberPoint>(points.size());
        for (var point : points) {
            NumberPoint.Value value =
                    switch (point.getValueCase()) {
                        case AS_INT -> NumberPoint.longValue(point.getAsInt());
                        case AS_DOUBLE -> NumberPoint.doubleValue(point.getAsDouble());
                        case VALUE_NOT_SET -> null;
                    };
            result.add(new NumberPoint(
                    CommonMapper.attributes(point.getAttributesList()),
                    point.getStartTimeUnixNano(),
                    point.getTimeUnixNano(),
                    value,
                    unsignedInt(point.getFlags())));
        }
        return result;
    }

    private static List<HistogramPoint> histogramPointsToDomain(List<HistogramDataPoint> points) {
        var result = new ArrayList<HistogramPoint>(points.size());
        for (var point : points) {
            result.add(new HistogramPoint(
                    CommonMapper.attributes(point.getAttributesList()),
                    point.getStartTimeUnixNano(),
                    point.getTimeUnixNano(),
                    point.getCount(),
                    point.hasSum() ? OptionalDouble.of(point.getSum()) : OptionalDouble.empty(),
                    point.getBucketCountsList(),
                    point.getExplicitBoundsList(),
                    point.hasMin() ? OptionalDouble.of(point.getMin()) : OptionalDouble.empty(),
                    point.hasMax() ? OptionalDouble.of(point.getMax()) : OptionalDouble.empty(),
                    unsignedInt(point.getFlags())));
        }
        return result;
    }

    private static List<ExponentialHistogramPoint> exponentialHistogramPointsToDomain(
            List<ExponentialHistogramDataPoint> points) {
        var result = new ArrayList<ExponentialHistogramPoint>(points.size());
        for (var point : points) {
            result.add(new ExponentialHistogramPoint(
                    CommonMapper.attributes(point.getAttributesList()),
                    point.getStartTimeUnixNano(),
                    point.getTimeUnixNano(),
                    point.getCount(),
                    point.hasSum() ? OptionalDouble.of(point.getSum()) : OptionalDouble.empty(),
                    point.getScale(),
                    point.getZeroCount(),
                    bucketsToDomain(point.getPositive()),
                    bucketsToDomain(point.getNegative()),
                    point.hasMin() ? OptionalDouble.of(point.getMin()) : OptionalDouble.empty(),
                    point.hasMax() ? OptionalDouble.of(point.getMax()) : OptionalDouble.empty(),
                    point.getZeroThreshold(),
                    unsignedInt(point.getFlags())));
        }
        return result;
    }

    private static ExponentialHistogramPoint.Buckets bucketsToDomain(
            ExponentialHistogramDataPoint.Buckets buckets) {
        // `offset` is its own wire field, distinguishable even when `bucket_counts` is empty,
        // so it must be preserved rather than collapsed to the zero-offset empty sentinel.
        return new ExponentialHistogramPoint.Buckets(
                buckets.getOffset(), buckets.getBucketCountsList());
    }

    private static List<SummaryPoint> summaryPointsToDomain(List<SummaryDataPoint> points) {
        var result = new ArrayList<SummaryPoint>(points.size());
        for (var point : points) {
            var quantiles = new ArrayList<SummaryPoint.Quantile>(point.getQuantileValuesCount());
            for (var quantile : point.getQuantileValuesList()) {
                quantiles.add(
                        new SummaryPoint.Quantile(quantile.getQuantile(), quantile.getValue()));
            }
            result.add(new SummaryPoint(
                    CommonMapper.attributes(point.getAttributesList()),
                    point.getStartTimeUnixNano(),
                    point.getTimeUnixNano(),
                    point.getCount(),
                    point.getSum(),
                    quantiles,
                    unsignedInt(point.getFlags())));
        }
        return result;
    }

    private static Metric.AggregationTemporality temporality(int number) {
        return number >= 0 && number < TEMPORALITIES.length
                ? TEMPORALITIES[number]
                : Metric.AggregationTemporality.UNSPECIFIED;
    }

    private static long unsignedInt(int value) {
        return value & 0xFFFFFFFFL;
    }

    /// Interprets an OTLP metrics export response as a [ConsumeResult].
    public static ConsumeResult<MetricsData> result(ExportMetricsServiceResponse response) {
        if (!response.hasPartialSuccess()) {
            return ConsumeResult.accepted();
        }
        var partial = response.getPartialSuccess();
        if (partial.getRejectedDataPoints() == 0 && partial.getErrorMessage().isEmpty()) {
            return ConsumeResult.accepted();
        }
        if (partial.getRejectedDataPoints() == 0) {
            return ConsumeResult.rejected(partial.getErrorMessage());
        }
        return ConsumeResult.partial(partial.getRejectedDataPoints(), partial.getErrorMessage());
    }

    // --- domain -> proto ---------------------------------------------------------------------

    public static ExportMetricsServiceRequest toProto(MetricsData metrics) {
        var request = ExportMetricsServiceRequest.newBuilder();
        for (var rm : metrics.resourceMetrics()) {
            request.addResourceMetrics(toProto(rm));
        }
        return request.build();
    }

    private static io.opentelemetry.proto.metrics.v1.ResourceMetrics toProto(
            MetricsData.ResourceMetrics rm) {
        var builder =
                io.opentelemetry.proto.metrics.v1.ResourceMetrics.newBuilder()
                        .setResource(CommonMapper.toProtoResource(rm.resource()))
                        .setSchemaUrl(rm.schemaUrl());
        for (var sm : rm.scopeMetrics()) {
            builder.addScopeMetrics(toProto(sm));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.metrics.v1.ScopeMetrics toProto(
            MetricsData.ScopeMetrics sm) {
        var builder =
                io.opentelemetry.proto.metrics.v1.ScopeMetrics.newBuilder()
                        .setScope(CommonMapper.toProtoScope(sm.scope()))
                        .setSchemaUrl(sm.schemaUrl());
        for (var metric : sm.metrics()) {
            builder.addMetrics(toProto(metric));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.metrics.v1.Metric toProto(Metric metric) {
        var builder =
                io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                        .setName(metric.name())
                        .setDescription(metric.description())
                        .setUnit(metric.unit())
                        .addAllMetadata(CommonMapper.toKeyValues(metric.metadata()));
        switch (metric.data()) {
            case Metric.Gauge gauge ->
                builder.setGauge(Gauge.newBuilder()
                        .addAllDataPoints(numberPointsToProto(gauge.points())));
            case Metric.Sum sum ->
                builder.setSum(Sum.newBuilder()
                        .addAllDataPoints(numberPointsToProto(sum.points()))
                        .setAggregationTemporality(temporality(sum.temporality()))
                        .setIsMonotonic(sum.monotonic()));
            case Metric.Histogram histogram ->
                builder.setHistogram(Histogram.newBuilder()
                        .addAllDataPoints(histogramPointsToProto(histogram.points()))
                        .setAggregationTemporality(temporality(histogram.temporality())));
            case Metric.ExponentialHistogram histogram ->
                builder.setExponentialHistogram(ExponentialHistogram.newBuilder()
                        .addAllDataPoints(exponentialHistogramPointsToProto(histogram.points()))
                        .setAggregationTemporality(temporality(histogram.temporality())));
            case Metric.Summary summary ->
                builder.setSummary(Summary.newBuilder()
                        .addAllDataPoints(summaryPointsToProto(summary.points())));
            case null -> {
                // metric carries no data points
            }
        }
        return builder.build();
    }

    private static List<NumberDataPoint> numberPointsToProto(List<NumberPoint> points) {
        var result = new ArrayList<NumberDataPoint>(points.size());
        for (var point : points) {
            var builder = NumberDataPoint.newBuilder()
                    .addAllAttributes(CommonMapper.toKeyValues(point.attributes()))
                    .setStartTimeUnixNano(point.startEpochNanos())
                    .setTimeUnixNano(point.epochNanos())
                    .setFlags((int) point.flags());
            switch (point.value()) {
                case NumberPoint.LongValue v -> builder.setAsInt(v.value());
                case NumberPoint.DoubleValue v -> builder.setAsDouble(v.value());
                case null -> {
                    // point has no recorded value
                }
            }
            result.add(builder.build());
        }
        return result;
    }

    private static List<HistogramDataPoint> histogramPointsToProto(List<HistogramPoint> points) {
        var result = new ArrayList<HistogramDataPoint>(points.size());
        for (var point : points) {
            var builder = HistogramDataPoint.newBuilder()
                    .addAllAttributes(CommonMapper.toKeyValues(point.attributes()))
                    .setStartTimeUnixNano(point.startEpochNanos())
                    .setTimeUnixNano(point.epochNanos())
                    .setCount(point.count())
                    .addAllBucketCounts(point.bucketCounts())
                    .addAllExplicitBounds(point.explicitBounds())
                    .setFlags((int) point.flags());
            point.sum().ifPresent(builder::setSum);
            point.min().ifPresent(builder::setMin);
            point.max().ifPresent(builder::setMax);
            result.add(builder.build());
        }
        return result;
    }

    private static List<ExponentialHistogramDataPoint> exponentialHistogramPointsToProto(
            List<ExponentialHistogramPoint> points) {
        var result = new ArrayList<ExponentialHistogramDataPoint>(points.size());
        for (var point : points) {
            var builder = ExponentialHistogramDataPoint.newBuilder()
                    .addAllAttributes(CommonMapper.toKeyValues(point.attributes()))
                    .setStartTimeUnixNano(point.startEpochNanos())
                    .setTimeUnixNano(point.epochNanos())
                    .setCount(point.count())
                    .setScale(point.scale())
                    .setZeroCount(point.zeroCount())
                    .setPositive(bucketsToProto(point.positive()))
                    .setNegative(bucketsToProto(point.negative()))
                    .setZeroThreshold(point.zeroThreshold())
                    .setFlags((int) point.flags());
            point.sum().ifPresent(builder::setSum);
            point.min().ifPresent(builder::setMin);
            point.max().ifPresent(builder::setMax);
            result.add(builder.build());
        }
        return result;
    }

    private static ExponentialHistogramDataPoint.Buckets bucketsToProto(
            ExponentialHistogramPoint.Buckets buckets) {
        return ExponentialHistogramDataPoint.Buckets.newBuilder()
                .setOffset(buckets.offset())
                .addAllBucketCounts(buckets.bucketCounts())
                .build();
    }

    private static List<SummaryDataPoint> summaryPointsToProto(List<SummaryPoint> points) {
        var result = new ArrayList<SummaryDataPoint>(points.size());
        for (var point : points) {
            var builder = SummaryDataPoint.newBuilder()
                    .addAllAttributes(CommonMapper.toKeyValues(point.attributes()))
                    .setStartTimeUnixNano(point.startEpochNanos())
                    .setTimeUnixNano(point.epochNanos())
                    .setCount(point.count())
                    .setSum(point.sum())
                    .setFlags((int) point.flags());
            for (var quantile : point.quantileValues()) {
                builder.addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder()
                        .setQuantile(quantile.quantile())
                        .setValue(quantile.value()));
            }
            result.add(builder.build());
        }
        return result;
    }

    private static AggregationTemporality temporality(Metric.AggregationTemporality temporality) {
        return AggregationTemporality.forNumber(temporality.ordinal());
    }
}
