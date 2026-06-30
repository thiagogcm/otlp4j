package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.LogsData.ResourceLogs;
import dev.nthings.otlp4j.model.LogsData.ScopeLogs;
import dev.nthings.otlp4j.model.MetricsData.ResourceMetrics;
import dev.nthings.otlp4j.model.MetricsData.ScopeMetrics;
import dev.nthings.otlp4j.model.ProfilesData.Profile;
import dev.nthings.otlp4j.model.ProfilesData.ResourceProfiles;
import dev.nthings.otlp4j.model.ProfilesData.ScopeProfiles;
import dev.nthings.otlp4j.model.TracesData.ResourceSpans;
import dev.nthings.otlp4j.model.TracesData.ScopeSpans;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box tests pinning the model records' real logic: flattening across resources/scopes,
/// defensive list copying in the compact constructors, and the documented constants.
@DisplayName("Model record invariants")
class ModelRecordInvariantsTest {

    // --- flattening across resources/scopes -------------------------------------------------

    @DisplayName("TracesData.spans flattens across every resource and scope")
    @Test
    void traceDataSpansFlattensAcrossEveryResourceAndScope() {
        var trace = new TracesData(List.of(
                new ResourceSpans(Resource.EMPTY, "", List.of(
                        new ScopeSpans(InstrumentationScope.EMPTY, "", List.of(
                                span("a"), span("b"))),
                        new ScopeSpans(InstrumentationScope.EMPTY, "", List.of(span("c"))))),
                new ResourceSpans(Resource.EMPTY, "", List.of(
                        new ScopeSpans(InstrumentationScope.EMPTY, "", List.of(span("d")))))));

        assertThat(trace.spans()).extracting(Span::name).containsExactly("a", "b", "c", "d");
    }

    @DisplayName("MetricsData.metrics flattens across every resource and scope")
    @Test
    void metricsDataMetricsFlattensAcrossEveryResourceAndScope() {
        var metrics = new MetricsData(List.of(
                new ResourceMetrics(Resource.EMPTY, "", List.of(
                        new ScopeMetrics(InstrumentationScope.EMPTY, "", List.of(
                                metric("m1"), metric("m2"))))),
                new ResourceMetrics(Resource.EMPTY, "", List.of(
                        new ScopeMetrics(InstrumentationScope.EMPTY, "", List.of(metric("m3")))))));

        assertThat(metrics.metrics()).extracting(Metric::name).containsExactly("m1", "m2", "m3");
    }

    @DisplayName("LogsData.logRecords flattens across every resource and scope")
    @Test
    void logsDataLogRecordsFlattensAcrossEveryResourceAndScope() {
        var logs = new LogsData(List.of(
                new ResourceLogs(Resource.EMPTY, "", List.of(
                        new ScopeLogs(InstrumentationScope.EMPTY, "", List.of(
                                logRecord("first"), logRecord("second"))))),
                new ResourceLogs(Resource.EMPTY, "", List.of(
                        new ScopeLogs(InstrumentationScope.EMPTY, "", List.of(logRecord("third")))))));

        assertThat(logs.logRecords())
                .extracting(LogRecord::eventName)
                .containsExactly("first", "second", "third");
    }

    @DisplayName("ProfilesData.profiles flattens across every resource and scope")
    @Test
    void profilesDataProfilesFlattensAcrossEveryResourceAndScope() {
        var profiles = new ProfilesData(List.of(
                new ResourceProfiles(Resource.EMPTY, "", List.of(
                        new ScopeProfiles(InstrumentationScope.EMPTY, "", List.of(
                                profile("p1"), profile("p2"))))),
                new ResourceProfiles(Resource.EMPTY, "", List.of(
                        new ScopeProfiles(InstrumentationScope.EMPTY, "", List.of(profile("p3")))))), new byte[0]);

        assertThat(profiles.profiles())
                .extracting(Profile::profileId)
                .containsExactly("p1", "p2", "p3");
    }

    // --- defensive list copying in compact constructors -------------------------------------

    @DisplayName("TracesData defensively copies its ResourceSpans list")
    @Test
    void traceDataDefensivelyCopiesItsResourceSpansList() {
        var source = new ArrayList<ResourceSpans>();
        var trace = new TracesData(source);

        source.add(new ResourceSpans(Resource.EMPTY, "", List.of()));

        assertThat(trace.resourceSpans()).isEmpty();
        assertThatThrownBy(() -> trace.resourceSpans().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @DisplayName("Span defensively copies its events and links lists")
    @Test
    void spanDefensivelyCopiesItsEventsAndLinksLists() {
        var events = new ArrayList<Span.Event>();
        var links = new ArrayList<Span.Link>();
        var span = Span.builder().name("op").events(events).links(links).build();

        events.add(new Span.Event(1L, "late", Attributes.empty(), 0));
        links.add(new Span.Link(
                "0102030405060708090a0b0c0d0e0f10", "0102030405060708", "", Attributes.empty(), 0, 0L));

        assertThat(span.events()).isEmpty();
        assertThat(span.links()).isEmpty();
        assertThatThrownBy(() -> span.events().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> span.links().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @DisplayName("Metric data kinds defensively copy their point lists")
    @Test
    void metricPointBearingDataKindsDefensivelyCopyTheirPointLists() {
        var numberPoints = new ArrayList<NumberPoint>();
        var gauge = new Metric.Gauge(numberPoints);
        var sum = new Metric.Sum(numberPoints, Metric.AggregationTemporality.DELTA, true);
        numberPoints.add(new NumberPoint(Attributes.empty(), 0, 0, NumberPoint.longValue(1), 0, List.of()));

        assertThat(gauge.points()).isEmpty();
        assertThat(sum.points()).isEmpty();
        assertThatThrownBy(() -> gauge.points().add(null))
                .isInstanceOf(UnsupportedOperationException.class);

        var histogramPoints = new ArrayList<HistogramPoint>();
        var histogram = new Metric.Histogram(histogramPoints, Metric.AggregationTemporality.CUMULATIVE);
        histogramPoints.add(histogramPoint());
        assertThat(histogram.points()).isEmpty();

        var expPoints = new ArrayList<ExponentialHistogramPoint>();
        var expHistogram =
                new Metric.ExponentialHistogram(expPoints, Metric.AggregationTemporality.DELTA);
        expPoints.add(exponentialHistogramPoint());
        assertThat(expHistogram.points()).isEmpty();

        var summaryPoints = new ArrayList<SummaryPoint>();
        var summary = new Metric.Summary(summaryPoints);
        summaryPoints.add(new SummaryPoint(Attributes.empty(), 0, 0, 0, 0.0, List.of(), 0));
        assertThat(summary.points()).isEmpty();
    }

    @DisplayName("HistogramPoint defensively copies its bucket and bound lists")
    @Test
    void histogramPointDefensivelyCopiesItsBucketAndBoundLists() {
        var bucketCounts = new ArrayList<Long>(List.of(1L, 2L));
        var explicitBounds = new ArrayList<Double>(List.of(0.5));
        var point = new HistogramPoint(
                Attributes.empty(), 0, 0, 3, OptionalDouble.empty(),
                bucketCounts, explicitBounds, OptionalDouble.empty(), OptionalDouble.empty(), 0, List.of());

        bucketCounts.add(99L);
        explicitBounds.add(99.0);

        assertThat(point.bucketCounts()).containsExactly(1L, 2L);
        assertThat(point.explicitBounds()).containsExactly(0.5);
        assertThatThrownBy(() -> point.bucketCounts().add(0L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @DisplayName("ExponentialHistogramPoint.Buckets defensively copies its counts list")
    @Test
    void exponentialHistogramBucketsDefensivelyCopiesItsCountsList() {
        var counts = new ArrayList<Long>(List.of(1L, 2L, 3L));
        var buckets = new ExponentialHistogramPoint.Buckets(4, counts);

        counts.add(99L);

        assertThat(buckets.bucketCounts()).containsExactly(1L, 2L, 3L);
        assertThatThrownBy(() -> buckets.bucketCounts().add(0L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @DisplayName("SummaryPoint defensively copies its quantile list")
    @Test
    void summaryPointDefensivelyCopiesItsQuantileList() {
        var quantiles = new ArrayList<SummaryPoint.Quantile>();
        quantiles.add(new SummaryPoint.Quantile(0.5, 1.0));
        var point = new SummaryPoint(Attributes.empty(), 0, 0, 1, 1.0, quantiles, 0);

        quantiles.add(new SummaryPoint.Quantile(0.9, 9.0));

        assertThat(point.quantileValues()).hasSize(1);
        assertThatThrownBy(() -> point.quantileValues().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- documented constants ---------------------------------------------------------------

    @DisplayName("Documented constants expose their empty or unset values")
    @Test
    void documentedConstantsExposeTheirEmptyOrUnsetValues() {
        assertThat(Resource.EMPTY.attributes()).isEqualTo(Attributes.empty());
        assertThat(Resource.EMPTY.droppedAttributesCount()).isZero();

        assertThat(InstrumentationScope.EMPTY.name()).isEmpty();
        assertThat(InstrumentationScope.EMPTY.version()).isEmpty();
        assertThat(InstrumentationScope.EMPTY.attributes()).isEqualTo(Attributes.empty());

        assertThat(Span.Status.UNSET.code()).isEqualTo(Span.Status.Code.UNSET);
        assertThat(Span.Status.UNSET.message()).isEmpty();

        assertThat(ExponentialHistogramPoint.Buckets.EMPTY.offset()).isZero();
        assertThat(ExponentialHistogramPoint.Buckets.EMPTY.bucketCounts()).isEmpty();

        assertThat(AttributeValue.EMPTY).isInstanceOf(AttributeValue.Empty.class);
    }

    @DisplayName("Exemplar validates required fields while preserving unset values")
    @Test
    void exemplarValidatesRequiredFieldsWhilePreservingUnsetValues() {
        assertThat(new Exemplar(Attributes.empty(), 5_000L, null, "", "").value()).isNull();
        assertThatThrownBy(() -> new Exemplar(null, 5_000L, null, "", ""))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("filteredAttributes");
        assertThatThrownBy(() -> new Exemplar(
                        Attributes.empty(), 5_000L, null, "not-hex", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spanId");
        assertThatThrownBy(() -> new Exemplar(
                        Attributes.empty(), 5_000L, null, "", "not-hex"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traceId");
    }

    // --- helpers ----------------------------------------------------------------------------

    private static Span span(String name) {
        return Span.builder().name(name).build();
    }

    private static Metric metric(String name) {
        return Metric.builder().name(name).data(new Metric.Gauge(List.of())).build();
    }

    private static LogRecord logRecord(String eventName) {
        return LogRecord.builder().eventName(eventName).build();
    }

    private static Profile profile(String profileId) {
        return new Profile(profileId, 0L, 0L, 0L, 0, 0, "", new byte[0]);
    }

    private static HistogramPoint histogramPoint() {
        return new HistogramPoint(
                Attributes.empty(), 0, 0, 0, OptionalDouble.empty(),
                List.of(), List.of(), OptionalDouble.empty(), OptionalDouble.empty(), 0, List.of());
    }

    private static ExponentialHistogramPoint exponentialHistogramPoint() {
        return new ExponentialHistogramPoint(
                Attributes.empty(), 0, 0, 0, OptionalDouble.empty(), 0, 0,
                ExponentialHistogramPoint.Buckets.EMPTY, ExponentialHistogramPoint.Buckets.EMPTY,
                OptionalDouble.empty(), OptionalDouble.empty(), 0.0, 0, List.of());
    }
}
