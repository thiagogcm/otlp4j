package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [SignalDescriptor#split] primitive, the inverse of merge: each signal partitions
/// a batch into item-capped sub-batches, item totals and wire order are preserved, a batch at or under
/// the cap returns a singleton of the same batch, and profiles split under a shared dictionary is
/// lossless.
class SignalDescriptorSplitTest {

    @Test
    @DisplayName("a non-positive cap is rejected")
    void nonPositiveCapRejected() {
        var batch = traces(3);
        assertThatThrownBy(() -> SignalDescriptor.TRACES.split(batch, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> SignalDescriptor.TRACES.split(batch, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a batch at or under the cap retains its contents")
    void atOrUnderCapReturnsSameBatch() {
        var batch = traces(5);
        assertThat(SignalDescriptor.TRACES.split(batch, 5)).containsExactly(batch);
        assertThat(SignalDescriptor.TRACES.split(batch, 8)).containsExactly(batch);
        assertThat(SignalDescriptor.TRACES.split(SignalDescriptor.TRACES.empty(), 4))
                .containsExactly(SignalDescriptor.TRACES.empty());
    }

    @Test
    @DisplayName("traces split into item-capped sub-batches, preserving totals and wire order")
    void tracesSplit() {
        var batch = traces(10);
        var parts = SignalDescriptor.TRACES.split(batch, 3);

        assertThat(parts).hasSize(4);
        assertThat(parts).allSatisfy(p -> assertThat(SignalDescriptor.TRACES.itemCount(p)).isLessThanOrEqualTo(3));
        assertThat(parts.stream().mapToLong(SignalDescriptor.TRACES::itemCount).sum()).isEqualTo(10);

        var names = new ArrayList<String>();
        parts.forEach(p -> p.forEachSpan(s -> names.add(s.name())));
        assertThat(names).containsExactly("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9");
    }

    @Test
    @DisplayName("traces split preserves resource and scope grouping across multiple resources")
    void tracesSplitMultipleResources() {
        var first = TracesData.of(resource("a"), InstrumentationScope.of("s1"), spans(0, 4)).resourceSpans();
        var second = TracesData.of(resource("b"), InstrumentationScope.of("s2"), spans(4, 8)).resourceSpans();
        var combined = new ArrayList<TracesData.ResourceSpans>();
        combined.addAll(first);
        combined.addAll(second);
        var batch = new TracesData(combined);

        var parts = SignalDescriptor.TRACES.split(batch, 3);
        assertThat(parts.stream().mapToLong(SignalDescriptor.TRACES::itemCount).sum()).isEqualTo(8);
        assertThat(parts).allSatisfy(p -> assertThat(SignalDescriptor.TRACES.itemCount(p)).isLessThanOrEqualTo(3));
        parts.forEach(p -> p.resourceSpans().forEach(rs -> rs.scopeSpans().forEach(ss -> {
            for (var span : ss.spans()) {
                var belongsToFirst = Integer.parseInt(span.name().substring(1)) < 4;
                assertThat(rs.resource().attributes().getString("service.name"))
                        .isEqualTo(belongsToFirst ? "a" : "b");
                assertThat(ss.scope().name()).isEqualTo(belongsToFirst ? "s1" : "s2");
            }
        })));
    }

    @Test
    @DisplayName("logs split into item-capped sub-batches, preserving totals")
    void logsSplit() {
        var records = new ArrayList<LogRecord>();
        for (var i = 0; i < 7; i++) {
            records.add(LogRecord.builder().eventName("e" + i).build());
        }
        var batch = LogsData.of(Resource.EMPTY, InstrumentationScope.of("lib"), records);

        var parts = SignalDescriptor.LOGS.split(batch, 2);
        assertThat(parts).hasSize(4);
        assertThat(parts.stream().mapToLong(SignalDescriptor.LOGS::itemCount).sum()).isEqualTo(7);
        assertThat(parts).allSatisfy(p -> assertThat(SignalDescriptor.LOGS.itemCount(p)).isLessThanOrEqualTo(2));
        assertThat(parts.stream().flatMap(p -> p.logRecords().stream()).map(LogRecord::eventName))
                .containsExactly("e0", "e1", "e2", "e3", "e4", "e5", "e6");
    }

    @Test
    @DisplayName("metrics split at the data-point level across metric variants, preserving point totals")
    void metricsSplitByDataPoint() {
        var gauge = Metric.builder().name("g").data(new Metric.Gauge(points(0, 4))).build();
        var sum = Metric.builder().name("s")
                .data(new Metric.Sum(points(4, 8), Metric.AggregationTemporality.DELTA, true)).build();
        var batch = MetricsData.of(Resource.EMPTY, InstrumentationScope.of("lib"), List.of(gauge, sum));
        assertThat(batch.dataPointCount()).isEqualTo(8);

        var parts = SignalDescriptor.METRICS.split(batch, 3);
        assertThat(parts.stream().mapToLong(SignalDescriptor.METRICS::itemCount).sum()).isEqualTo(8);
        assertThat(parts).allSatisfy(p -> assertThat(SignalDescriptor.METRICS.itemCount(p)).isLessThanOrEqualTo(3));
        var values = new ArrayList<NumberPoint.Value>();
        parts.forEach(part -> part.forEachMetric(metric -> {
            var points = switch (metric.dataOrThrow()) {
                case Metric.Gauge data -> data.points();
                case Metric.Sum data -> data.points();
                default -> throw new AssertionError("unexpected metric data");
            };
            points.forEach(point -> values.add(point.value()));
            assertThat(metric.name()).isEqualTo(metric.dataOrThrow() instanceof Metric.Gauge ? "g" : "s");
        }));
        assertThat(values).containsExactly(
                NumberPoint.longValue(0), NumberPoint.longValue(1), NumberPoint.longValue(2), NumberPoint.longValue(3),
                NumberPoint.longValue(4), NumberPoint.longValue(5), NumberPoint.longValue(6), NumberPoint.longValue(7));
    }

    @Test
    @DisplayName("metrics split preserves a zero-point (DATA_NOT_SET) metric interspersed with data")
    void metricsSplitPreservesNoDataMetric() {
        var head = Metric.builder().name("g").data(new Metric.Gauge(points(0, 4))).build();
        var empty = Metric.builder().name("nodata").build();
        var tail = Metric.builder().name("h")
                .data(new Metric.Histogram(List.of(), Metric.AggregationTemporality.CUMULATIVE)).build();
        var full = Metric.builder().name("g2").data(new Metric.Gauge(points(4, 8))).build();
        var batch = MetricsData.of(
                Resource.EMPTY, InstrumentationScope.of("lib"), List.of(head, empty, tail, full));

        var parts = SignalDescriptor.METRICS.split(batch, 3);
        assertThat(parts.stream().mapToLong(SignalDescriptor.METRICS::itemCount).sum()).isEqualTo(8);
        var allMetrics = parts.stream().flatMap(p -> p.metrics().stream()).toList();
        assertThat(allMetrics.stream().filter(m -> !m.hasData()).toList()).containsExactly(empty);
        assertThat(allMetrics.stream().filter(m -> m.name().equals("h")).toList()).containsExactly(tail);
    }

    @Test
    @DisplayName("profiles split under a shared dictionary is lossless and carries the dictionary")
    void profilesSplitSharedDictionary() {
        var dictionary = new byte[] {7, 8, 9};
        var profiles = new ArrayList<ProfilesData.Profile>();
        for (var i = 0; i < 5; i++) {
            profiles.add(new ProfilesData.Profile("p" + i, 0L, 0L, 0L, 0, 0, "", new byte[0]));
        }
        var scoped = ProfilesData.of(Resource.EMPTY, InstrumentationScope.of("lib"), profiles).resourceProfiles();
        var batch = new ProfilesData(scoped, dictionary);

        var parts = SignalDescriptor.PROFILES.split(batch, 2);
        assertThat(parts).hasSize(3);
        assertThat(parts.stream().mapToLong(SignalDescriptor.PROFILES::itemCount).sum()).isEqualTo(5);
        assertThat(parts).allSatisfy(p -> assertThat(p.dictionary()).containsExactly(7, 8, 9));
        assertThat(parts.stream().flatMap(p -> p.profiles().stream()).map(ProfilesData.Profile::profileId))
                .containsExactly("p0", "p1", "p2", "p3", "p4");
    }

    private static TracesData traces(int n) {
        return TracesData.of(Resource.EMPTY, InstrumentationScope.of("lib"), spans(0, n));
    }

    private static List<Span> spans(int from, int to) {
        var spans = new ArrayList<Span>();
        for (var i = from; i < to; i++) {
            spans.add(Span.builder().name("s" + i).build());
        }
        return spans;
    }

    private static List<NumberPoint> points(int from, int to) {
        var points = new ArrayList<NumberPoint>();
        for (var i = from; i < to; i++) {
            points.add(NumberPoint.of(Attributes.empty(), i, NumberPoint.longValue(i)));
        }
        return points;
    }

    private static Resource resource(String service) {
        return Resource.of(Attributes.builder().put("service.name", service).build());
    }
}
