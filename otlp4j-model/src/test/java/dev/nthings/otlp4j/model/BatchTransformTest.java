package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the item-level and resource-level `map…` and `filter…` operations on each signal batch.
/// The traces cases cover the shared rewrite rules: order, pruning of emptied groups, retention of
/// untouched groups, and the same-instance result for a rewrite that changes nothing. The other
/// signals check that their operations address the right level and carry their own group metadata.
@DisplayName("Batch map and filter operations")
class BatchTransformTest {

    private static final Resource CHECKOUT = resource("checkout");
    private static final Resource PAYMENTS = resource("payments");

    @Nested
    @DisplayName("TracesData")
    class Traces {

        private final TracesData.ScopeSpans http = new TracesData.ScopeSpans(
                InstrumentationScope.of("http"), "scope-url", List.of(span("a"), span("b")));
        private final TracesData.ScopeSpans db = new TracesData.ScopeSpans(
                InstrumentationScope.of("db"), "", List.of(span("c")));
        private final TracesData.ScopeSpans idle = new TracesData.ScopeSpans(
                InstrumentationScope.of("idle"), "", List.of());
        private final TracesData.ResourceSpans checkout =
                new TracesData.ResourceSpans(CHECKOUT, "resource-url", List.of(http, db, idle));
        private final TracesData.ResourceSpans payments = new TracesData.ResourceSpans(
                PAYMENTS, "", List.of(new TracesData.ScopeSpans(
                        InstrumentationScope.of("http"), "", List.of(span("d"), span("e")))));
        private final TracesData data = new TracesData(List.of(checkout, payments));

        @DisplayName("filterSpans keeps accepted spans in order under their original groups")
        @Test
        void filterSpansKeepsAcceptedSpansUnderTheirGroups() {
            var kept = data.filterSpans(span -> !span.name().equals("b") && !span.name().equals("d"));

            assertThat(kept.spans()).extracting(Span::name).containsExactly("a", "c", "e");
            var first = kept.resourceSpans().get(0);
            assertThat(first.resource()).isEqualTo(CHECKOUT);
            assertThat(first.schemaUrl()).isEqualTo("resource-url");
            assertThat(first.scopeSpans().get(0).scope()).isEqualTo(InstrumentationScope.of("http"));
            assertThat(first.scopeSpans().get(0).schemaUrl()).isEqualTo("scope-url");
            assertThat(kept.resourceSpans().get(1).resource()).isEqualTo(PAYMENTS);
        }

        @DisplayName("filterSpans removes emptied scopes and resources and keeps untouched groups")
        @Test
        void filterSpansRemovesEmptiedGroups() {
            var kept = data.filterSpans(span -> span.name().equals("c"));

            assertThat(kept.resourceSpans()).hasSize(1);
            // The emptied http scope is gone; the untouched db and already-empty idle scopes remain.
            assertThat(kept.resourceSpans().get(0).scopeSpans()).containsExactly(db, idle);
        }

        @DisplayName("filterSpans that rejects every span leaves no resource group")
        @Test
        void filterSpansRejectingEverythingLeavesAnEmptyBatch() {
            var single = new TracesData(List.of(payments));

            assertThat(single.filterSpans(span -> false).resourceSpans()).isEmpty();
        }

        @DisplayName("filterSpans that accepts every span returns the same batch")
        @Test
        void filterSpansAcceptingEverythingReturnsTheSameBatch() {
            assertThat(data.filterSpans(span -> true)).isSameAs(data);
        }

        @DisplayName("mapSpans replaces spans in place and keeps every group")
        @Test
        void mapSpansReplacesSpansInPlace() {
            var renamed = data.mapSpans(span -> span.name().equals("c")
                    ? span.toBuilder().name("C").build()
                    : span);

            assertThat(renamed.spans()).extracting(Span::name).containsExactly("a", "b", "C", "d", "e");
            assertThat(renamed.resourceSpans().get(0).scopeSpans())
                    .extracting(TracesData.ScopeSpans::scope)
                    .containsExactly(http.scope(), db.scope(), idle.scope());
            // Groups without a replaced span are shared rather than copied.
            assertThat(renamed.resourceSpans().get(0).scopeSpans().get(0)).isSameAs(http);
            assertThat(renamed.resourceSpans().get(1)).isSameAs(payments);
        }

        @DisplayName("mapSpans that returns every span unchanged returns the same batch")
        @Test
        void mapSpansChangingNothingReturnsTheSameBatch() {
            assertThat(data.mapSpans(span -> span)).isSameAs(data);
        }

        @DisplayName("mapSpans rejects a null replacement")
        @Test
        void mapSpansRejectsANullReplacement() {
            assertThatNullPointerException().isThrownBy(() -> data.mapSpans(span -> null));
        }

        @DisplayName("mapResources replaces resources and keeps schema URLs and spans")
        @Test
        void mapResourcesReplacesResources() {
            var enriched = data.mapResources(resource -> resource.withAttribute("env", AttributeValue.of("prod")));

            assertThat(enriched.resourceSpans())
                    .extracting(group -> group.resource().attributes().getString("env"))
                    .containsExactly("prod", "prod");
            assertThat(enriched.resourceSpans().get(0).resource().attributes().getString("service.name"))
                    .isEqualTo("checkout");
            assertThat(enriched.resourceSpans().get(0).schemaUrl()).isEqualTo("resource-url");
            assertThat(enriched.resourceSpans().get(0).scopeSpans()).containsExactly(http, db, idle);
            assertThat(data.mapResources(resource -> resource)).isSameAs(data);
        }

        @DisplayName("filterResources removes a rejected resource with its spans")
        @Test
        void filterResourcesRemovesRejectedResources() {
            var kept = data.filterResources(resource -> resource.equals(PAYMENTS));

            assertThat(kept.resourceSpans()).containsExactly(payments);
            assertThat(kept.spans()).extracting(Span::name).containsExactly("d", "e");
            assertThat(data.filterResources(resource -> true)).isSameAs(data);
        }

        private static Span span(String name) {
            return Span.builder().name(name).build();
        }
    }

    @Nested
    @DisplayName("MetricsData")
    class Metrics {

        private final MetricsData.ResourceMetrics checkout = new MetricsData.ResourceMetrics(
                CHECKOUT, "resource-url", List.of(
                        new MetricsData.ScopeMetrics(
                                InstrumentationScope.of("http"), "scope-url", List.of(metric("a", 2), metric("b", 1))),
                        new MetricsData.ScopeMetrics(InstrumentationScope.of("db"), "", List.of(metric("c", 1)))));
        private final MetricsData.ResourceMetrics payments = new MetricsData.ResourceMetrics(
                PAYMENTS, "", List.of(new MetricsData.ScopeMetrics(
                        InstrumentationScope.of("http"), "", List.of(metric("d", 3)))));
        private final MetricsData data = new MetricsData(List.of(checkout, payments));

        @DisplayName("filterMetrics keeps accepted metrics with all their points and prunes emptied groups")
        @Test
        void filterMetricsKeepsWholeMetricsAndPrunesEmptiedGroups() {
            var kept = data.filterMetrics(metric -> metric.name().equals("a"));

            assertThat(kept.resourceMetrics()).hasSize(1);
            var group = kept.resourceMetrics().get(0);
            assertThat(group.resource()).isEqualTo(CHECKOUT);
            assertThat(group.schemaUrl()).isEqualTo("resource-url");
            assertThat(group.scopeMetrics()).hasSize(1);
            assertThat(group.scopeMetrics().get(0).schemaUrl()).isEqualTo("scope-url");
            assertThat(kept.metrics()).extracting(Metric::name).containsExactly("a");
            assertThat(kept.dataPointCount()).isEqualTo(2);
            assertThat(data.filterMetrics(metric -> true)).isSameAs(data);
        }

        @DisplayName("mapMetrics replaces metrics in place")
        @Test
        void mapMetricsReplacesMetricsInPlace() {
            var renamed = data.mapMetrics(metric -> metric.name().equals("c")
                    ? metric.toBuilder().name("C").build()
                    : metric);

            assertThat(renamed.metrics()).extracting(Metric::name).containsExactly("a", "b", "C", "d");
            assertThat(renamed.resourceMetrics().get(0).scopeMetrics().get(1).scope())
                    .isEqualTo(InstrumentationScope.of("db"));
            assertThat(renamed.dataPointCount()).isEqualTo(data.dataPointCount());
            assertThat(data.mapMetrics(metric -> metric)).isSameAs(data);
        }

        @DisplayName("resource operations address resource groups")
        @Test
        void resourceOperationsAddressResourceGroups() {
            assertThat(data.filterResources(resource -> resource.equals(PAYMENTS)).resourceMetrics())
                    .containsExactly(payments);

            var enriched = data.mapResources(resource -> resource.withAttribute("env", AttributeValue.of("prod")));
            assertThat(enriched.resourceMetrics().get(0).resource().attributes().getString("env")).isEqualTo("prod");
            assertThat(enriched.resourceMetrics().get(0).schemaUrl()).isEqualTo("resource-url");
            assertThat(enriched.resourceMetrics().get(0).scopeMetrics()).isEqualTo(checkout.scopeMetrics());
        }

        private static Metric metric(String name, int points) {
            var point = new NumberPoint(Attributes.empty(), 0L, 1L, NumberPoint.longValue(1L), 0L, List.of());
            return Metric.builder()
                    .name(name)
                    .data(new Metric.Gauge(Collections.nCopies(points, point)))
                    .build();
        }
    }

    @Nested
    @DisplayName("LogsData")
    class Logs {

        private final LogsData.ResourceLogs checkout = new LogsData.ResourceLogs(
                CHECKOUT, "resource-url", List.of(
                        new LogsData.ScopeLogs(
                                InstrumentationScope.of("http"), "scope-url", List.of(record("a"), record("b"))),
                        new LogsData.ScopeLogs(InstrumentationScope.of("db"), "", List.of(record("c")))));
        private final LogsData.ResourceLogs payments = new LogsData.ResourceLogs(
                PAYMENTS, "", List.of(new LogsData.ScopeLogs(
                        InstrumentationScope.of("http"), "", List.of(record("d")))));
        private final LogsData data = new LogsData(List.of(checkout, payments));

        @DisplayName("filterLogRecords keeps accepted records and prunes emptied groups")
        @Test
        void filterLogRecordsKeepsAcceptedRecordsAndPrunesEmptiedGroups() {
            var kept = data.filterLogRecords(record -> record.eventName().equals("b"));

            assertThat(kept.resourceLogs()).hasSize(1);
            var group = kept.resourceLogs().get(0);
            assertThat(group.resource()).isEqualTo(CHECKOUT);
            assertThat(group.schemaUrl()).isEqualTo("resource-url");
            assertThat(group.scopeLogs()).hasSize(1);
            assertThat(group.scopeLogs().get(0).schemaUrl()).isEqualTo("scope-url");
            assertThat(kept.logRecords()).extracting(LogRecord::eventName).containsExactly("b");
            assertThat(data.filterLogRecords(record -> true)).isSameAs(data);
        }

        @DisplayName("mapLogRecords replaces records in place")
        @Test
        void mapLogRecordsReplacesRecordsInPlace() {
            var renamed = data.mapLogRecords(record -> record.eventName().equals("c")
                    ? record.toBuilder().eventName("C").build()
                    : record);

            assertThat(renamed.logRecords()).extracting(LogRecord::eventName).containsExactly("a", "b", "C", "d");
            assertThat(renamed.resourceLogs().get(0).scopeLogs().get(1).scope())
                    .isEqualTo(InstrumentationScope.of("db"));
            assertThat(data.mapLogRecords(record -> record)).isSameAs(data);
        }

        @DisplayName("resource operations address resource groups")
        @Test
        void resourceOperationsAddressResourceGroups() {
            assertThat(data.filterResources(resource -> resource.equals(PAYMENTS)).resourceLogs())
                    .containsExactly(payments);

            var enriched = data.mapResources(resource -> resource.withAttribute("env", AttributeValue.of("prod")));
            assertThat(enriched.resourceLogs().get(0).resource().attributes().getString("env")).isEqualTo("prod");
            assertThat(enriched.resourceLogs().get(0).schemaUrl()).isEqualTo("resource-url");
            assertThat(enriched.resourceLogs().get(0).scopeLogs()).isEqualTo(checkout.scopeLogs());
        }

        private static LogRecord record(String eventName) {
            return LogRecord.builder().eventName(eventName).build();
        }
    }

    @Nested
    @DisplayName("ProfilesData")
    class Profiles {

        private final byte[] dictionary = {1, 2, 3};
        private final ProfilesData.ResourceProfiles checkout = new ProfilesData.ResourceProfiles(
                CHECKOUT, "resource-url", List.of(
                        new ProfilesData.ScopeProfiles(
                                InstrumentationScope.of("cpu"), "scope-url", List.of(profile("a"), profile("b"))),
                        new ProfilesData.ScopeProfiles(InstrumentationScope.of("heap"), "", List.of(profile("c")))));
        private final ProfilesData.ResourceProfiles payments = new ProfilesData.ResourceProfiles(
                PAYMENTS, "", List.of(new ProfilesData.ScopeProfiles(
                        InstrumentationScope.of("cpu"), "", List.of(profile("d")))));
        private final ProfilesData data = new ProfilesData(List.of(checkout, payments), dictionary);

        @DisplayName("filterProfiles keeps accepted profiles, prunes emptied groups, and keeps the dictionary")
        @Test
        void filterProfilesKeepsAcceptedProfilesAndTheDictionary() {
            var kept = data.filterProfiles(profile -> profile.profileId().equals("b"));

            assertThat(kept.resourceProfiles()).hasSize(1);
            var group = kept.resourceProfiles().get(0);
            assertThat(group.resource()).isEqualTo(CHECKOUT);
            assertThat(group.schemaUrl()).isEqualTo("resource-url");
            assertThat(group.scopeProfiles()).hasSize(1);
            assertThat(group.scopeProfiles().get(0).schemaUrl()).isEqualTo("scope-url");
            assertThat(kept.profiles()).extracting(ProfilesData.Profile::profileId).containsExactly("b");
            assertThat(kept.dictionary()).isEqualTo(dictionary);
            assertThat(data.filterProfiles(profile -> true)).isSameAs(data);
        }

        @DisplayName("mapProfiles replaces profiles in place and keeps the dictionary")
        @Test
        void mapProfilesReplacesProfilesInPlace() {
            var renamed = data.mapProfiles(profile -> profile.profileId().equals("c") ? profile("C") : profile);

            assertThat(renamed.profiles())
                    .extracting(ProfilesData.Profile::profileId)
                    .containsExactly("a", "b", "C", "d");
            assertThat(renamed.resourceProfiles().get(0).scopeProfiles().get(1).scope())
                    .isEqualTo(InstrumentationScope.of("heap"));
            assertThat(renamed.dictionary()).isEqualTo(dictionary);
            assertThat(data.mapProfiles(profile -> profile)).isSameAs(data);
        }

        @DisplayName("resource operations address resource groups and keep the dictionary")
        @Test
        void resourceOperationsAddressResourceGroups() {
            var kept = data.filterResources(resource -> resource.equals(PAYMENTS));
            assertThat(kept.resourceProfiles()).containsExactly(payments);
            assertThat(kept.dictionary()).isEqualTo(dictionary);

            var enriched = data.mapResources(resource -> resource.withAttribute("env", AttributeValue.of("prod")));
            assertThat(enriched.resourceProfiles().get(0).resource().attributes().getString("env"))
                    .isEqualTo("prod");
            assertThat(enriched.resourceProfiles().get(0).schemaUrl()).isEqualTo("resource-url");
            assertThat(enriched.resourceProfiles().get(0).scopeProfiles()).isEqualTo(checkout.scopeProfiles());
            assertThat(enriched.dictionary()).isEqualTo(dictionary);
        }

        private static ProfilesData.Profile profile(String id) {
            return new ProfilesData.Profile(id, 0L, 0L, 0L, 0, 0, "", new byte[0]);
        }
    }

    private static Resource resource(String serviceName) {
        return Resource.of(Attributes.builder().put("service.name", serviceName).build());
    }
}
