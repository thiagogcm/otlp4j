package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the allocation-free traversal and item-count helpers on each
/// signal batch: `forEachSpan`/`spanCount` and their metric, log, and profile
/// counterparts. Each helper must visit (or count) the same items in the same
/// order as the flattened accessor, across a multi-resource/multi-scope batch
/// including empty scopes, and reject a null action.
@DisplayName("Signal traversal and count helpers")
class SignalTraversalTest {

    @Nested
    @DisplayName("TraceData")
    class Traces {

        private final TraceData data = new TraceData(List.of(
                new TraceData.ResourceSpans(
                        Resource.EMPTY,
                        "",
                        List.of(
                                new TraceData.ScopeSpans(
                                        InstrumentationScope.of("s1"), "", List.of(span("a"), span("b"))),
                                new TraceData.ScopeSpans(InstrumentationScope.of("s2"), "", List.of()),
                                new TraceData.ScopeSpans(InstrumentationScope.of("s3"), "", List.of(span("c"))))),
                // A resource with zero scopes: the outer loop must skip it without visiting
                // anything.
                new TraceData.ResourceSpans(Resource.EMPTY, "", List.of()),
                new TraceData.ResourceSpans(
                        Resource.EMPTY,
                        "",
                        List.of(new TraceData.ScopeSpans(
                                InstrumentationScope.of("s4"), "", List.of(span("d"), span("e")))))));

        @DisplayName("forEachSpan visits every span in flattened order")
        @Test
        void forEachSpanVisitsEverySpanInFlattenedOrder() {
            var visited = new ArrayList<Span>();

            data.forEachSpan(visited::add);

            assertThat(visited).containsExactlyElementsOf(data.spans());
            assertThat(visited).extracting(Span::name).containsExactly("a", "b", "c", "d", "e");
        }

        @DisplayName("spanCount equals the flattened span count")
        @Test
        void spanCountEqualsFlattenedSpanCount() {
            assertThat(data.spanCount()).isEqualTo(data.spans().size()).isEqualTo(5);
        }

        @DisplayName("an empty batch counts zero and never invokes the action")
        @Test
        void emptyBatchCountsZeroAndNeverInvokesTheAction() {
            var empty = new TraceData(List.of());

            assertThat(empty.spanCount()).isZero();
            empty.forEachSpan(span -> {
                throw new AssertionError("should not be called");
            });
        }

        @DisplayName("forEachSpan rejects a null action")
        @Test
        void forEachSpanRejectsANullAction() {
            assertThatNullPointerException().isThrownBy(() -> data.forEachSpan(null));
        }

        private static Span span(String name) {
            return Span.builder().name(name).build();
        }
    }

    @Nested
    @DisplayName("MetricsData")
    class Metrics {

        private final MetricsData data = new MetricsData(List.of(
                new MetricsData.ResourceMetrics(
                        Resource.EMPTY,
                        "",
                        List.of(
                                new MetricsData.ScopeMetrics(
                                        InstrumentationScope.of("s1"), "", List.of(metric("a"), metric("b"))),
                                new MetricsData.ScopeMetrics(InstrumentationScope.of("s2"), "", List.of()))),
                new MetricsData.ResourceMetrics(
                        Resource.EMPTY,
                        "",
                        List.of(new MetricsData.ScopeMetrics(
                                InstrumentationScope.of("s3"), "", List.of(metric("c")))))));

        @DisplayName("forEachMetric visits every metric in flattened order")
        @Test
        void forEachMetricVisitsEveryMetricInFlattenedOrder() {
            var visited = new ArrayList<Metric>();

            data.forEachMetric(visited::add);

            assertThat(visited).containsExactlyElementsOf(data.metrics());
            assertThat(visited).extracting(Metric::name).containsExactly("a", "b", "c");
        }

        @DisplayName("forEachMetric never invokes the action on an empty batch")
        @Test
        void forEachMetricNeverInvokesTheActionOnAnEmptyBatch() {
            new MetricsData(List.of()).forEachMetric(metric -> {
                throw new AssertionError("should not be called");
            });
        }

        @DisplayName("forEachMetric rejects a null action")
        @Test
        void forEachMetricRejectsANullAction() {
            assertThatNullPointerException().isThrownBy(() -> data.forEachMetric(null));
        }

        @DisplayName("dataPointCount walks every Metric.Data kind")
        @Test
        void dataPointCountWalksEveryDataKind() {
            var point = new NumberPoint(Attributes.empty(), 0L, 1L, NumberPoint.longValue(1L), 0L, List.of());
            var rich = MetricsData.of(Resource.EMPTY, InstrumentationScope.of("s"), List.of(
                    Metric.builder().name("g").data(new Metric.Gauge(List.of(point))).build(),
                    Metric.builder().name("s")
                            .data(new Metric.Sum(List.of(), Metric.AggregationTemporality.DELTA, true)).build(),
                    Metric.builder().name("h")
                            .data(new Metric.Histogram(List.of(), Metric.AggregationTemporality.CUMULATIVE)).build(),
                    Metric.builder().name("e")
                            .data(new Metric.ExponentialHistogram(List.of(), Metric.AggregationTemporality.DELTA))
                            .build(),
                    Metric.builder().name("su").data(new Metric.Summary(List.of())).build(),
                    Metric.builder().name("nodata").build()));
            assertThat(rich.dataPointCount()).isEqualTo(1L);
            assertThat(new MetricsData(List.of()).dataPointCount()).isZero();
        }

        private static Metric metric(String name) {
            return Metric.builder().name(name).data(new Metric.Gauge(List.of())).build();
        }
    }

    @Nested
    @DisplayName("LogsData")
    class Logs {

        private final LogsData data = new LogsData(List.of(
                new LogsData.ResourceLogs(
                        Resource.EMPTY,
                        "",
                        List.of(
                                new LogsData.ScopeLogs(
                                        InstrumentationScope.of("s1"), "", List.of(record("a"), record("b"))),
                                new LogsData.ScopeLogs(InstrumentationScope.of("s2"), "", List.of()))),
                new LogsData.ResourceLogs(
                        Resource.EMPTY,
                        "",
                        List.of(new LogsData.ScopeLogs(
                                InstrumentationScope.of("s3"), "", List.of(record("c"), record("d")))))));

        @DisplayName("forEachLogRecord visits every record in flattened order")
        @Test
        void forEachLogRecordVisitsEveryRecordInFlattenedOrder() {
            var visited = new ArrayList<LogRecord>();

            data.forEachLogRecord(visited::add);

            assertThat(visited).containsExactlyElementsOf(data.logRecords());
            assertThat(visited).extracting(LogRecord::eventName).containsExactly("a", "b", "c", "d");
        }

        @DisplayName("logRecordCount equals the flattened record count")
        @Test
        void logRecordCountEqualsFlattenedRecordCount() {
            assertThat(data.logRecordCount()).isEqualTo(data.logRecords().size()).isEqualTo(4);
        }

        @DisplayName("an empty batch counts zero and never invokes the action")
        @Test
        void emptyBatchCountsZeroAndNeverInvokesTheAction() {
            var empty = new LogsData(List.of());

            assertThat(empty.logRecordCount()).isZero();
            empty.forEachLogRecord(record -> {
                throw new AssertionError("should not be called");
            });
        }

        @DisplayName("forEachLogRecord rejects a null action")
        @Test
        void forEachLogRecordRejectsANullAction() {
            assertThatNullPointerException().isThrownBy(() -> data.forEachLogRecord(null));
        }

        private static LogRecord record(String eventName) {
            return LogRecord.builder().eventName(eventName).build();
        }
    }

    @Nested
    @DisplayName("ProfilesData")
    class Profiles {

        private final ProfilesData data = new ProfilesData(
                List.of(
                        new ProfilesData.ResourceProfiles(
                                Resource.EMPTY,
                                "",
                                List.of(
                                        new ProfilesData.ScopeProfiles(
                                                InstrumentationScope.of("s1"),
                                                "",
                                                List.of(profile("a"), profile("b"))),
                                        new ProfilesData.ScopeProfiles(
                                                InstrumentationScope.of("s2"), "", List.of()))),
                        new ProfilesData.ResourceProfiles(
                                Resource.EMPTY,
                                "",
                                List.of(new ProfilesData.ScopeProfiles(
                                        InstrumentationScope.of("s3"), "", List.of(profile("c")))))),
                new byte[0]);

        @DisplayName("forEachProfile visits every profile in flattened order")
        @Test
        void forEachProfileVisitsEveryProfileInFlattenedOrder() {
            var visited = new ArrayList<ProfilesData.Profile>();

            data.forEachProfile(visited::add);

            assertThat(visited).containsExactlyElementsOf(data.profiles());
            assertThat(visited).extracting(ProfilesData.Profile::profileId).containsExactly("a", "b", "c");
        }

        @DisplayName("profileCount equals the flattened profile count")
        @Test
        void profileCountEqualsFlattenedProfileCount() {
            assertThat(data.profileCount()).isEqualTo(data.profiles().size()).isEqualTo(3);
        }

        @DisplayName("an empty batch counts zero and never invokes the action")
        @Test
        void emptyBatchCountsZeroAndNeverInvokesTheAction() {
            var empty = new ProfilesData(List.of(), new byte[0]);

            assertThat(empty.profileCount()).isZero();
            empty.forEachProfile(profile -> {
                throw new AssertionError("should not be called");
            });
        }

        @DisplayName("forEachProfile rejects a null action")
        @Test
        void forEachProfileRejectsANullAction() {
            assertThatNullPointerException().isThrownBy(() -> data.forEachProfile(null));
        }

        private static ProfilesData.Profile profile(String id) {
            return new ProfilesData.Profile(id, 0L, 0L, 0L, 0, 0, "", new byte[0]);
        }
    }
}
