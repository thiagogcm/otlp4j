package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for the convenience factories that spare callers from hand-nesting resource/scope
/// wrappers: [Resource#of], [InstrumentationScope#of], and the single-resource/single-scope
/// `of(...)` batch factories on each signal type.
@DisplayName("Model convenience factories")
class ModelFactoriesTest {

    @DisplayName("Resource.of carries attributes and a zero dropped count")
    @Test
    void resourceOfCarriesAttributesAndAZeroDroppedCount() {
        var attributes = Attributes.builder().put("service.name", "checkout").build();

        var resource = Resource.of(attributes);

        assertThat(resource.attributes()).isEqualTo(attributes);
        assertThat(resource.droppedAttributesCount()).isZero();
    }

    @DisplayName("InstrumentationScope.of fills name, with optional version and empty attributes")
    @Test
    void instrumentationScopeOfFillsNameWithOptionalVersionAndEmptyAttributes() {
        var nameOnly = InstrumentationScope.of("lib");
        assertThat(nameOnly.name()).isEqualTo("lib");
        assertThat(nameOnly.version()).isEmpty();
        assertThat(nameOnly.attributes()).isEqualTo(Attributes.empty());
        assertThat(nameOnly.droppedAttributesCount()).isZero();

        var withVersion = InstrumentationScope.of("lib", "2.0");
        assertThat(withVersion.name()).isEqualTo("lib");
        assertThat(withVersion.version()).isEqualTo("2.0");
        assertThat(withVersion.attributes()).isEqualTo(Attributes.empty());
    }

    @DisplayName("Resource.withAttribute adds or replaces one attribute, preserving dropped count")
    @Test
    void resourceWithAttributeAddsOrReplacesOneAttribute() {
        var base = new Resource(Attributes.builder().put("service.name", "old").build(), 3);

        var enriched = base.withAttribute("service.name", AttributeValue.of("checkout"))
                .withAttribute("env", AttributeValue.of("prod"));

        assertThat(enriched.attributes().getString("service.name")).isEqualTo("checkout");
        assertThat(enriched.attributes().getString("env")).isEqualTo("prod");
        assertThat(enriched.droppedAttributesCount()).isEqualTo(3);
        assertThat(base.attributes().getString("service.name"))
                .as("withAttribute must not mutate the source resource")
                .isEqualTo("old");
    }

    @DisplayName("InstrumentationScope.withAttribute adds an attribute, preserving name/version/dropped count")
    @Test
    void instrumentationScopeWithAttributeAddsAnAttribute() {
        var base = new InstrumentationScope("lib", "2.0", Attributes.empty(), 1);

        var enriched = base.withAttribute("team", AttributeValue.of("payments"));

        assertThat(enriched.name()).isEqualTo("lib");
        assertThat(enriched.version()).isEqualTo("2.0");
        assertThat(enriched.droppedAttributesCount()).isEqualTo(1);
        assertThat(enriched.attributes().getString("team")).isEqualTo("payments");
        assertThat(base.attributes().contains("team"))
                .as("withAttribute must not mutate the source scope")
                .isFalse();
    }

    @DisplayName("TracesData.of wraps spans under one resource and scope")
    @Test
    void traceDataOfWrapsSpansUnderOneResourceAndScope() {
        var resource = Resource.of(Attributes.builder().put("k", "v").build());
        var scope = InstrumentationScope.of("lib", "1.0");
        var span = Span.builder().name("op").build();

        var data = TracesData.of(resource, scope, List.of(span));

        assertThat(data.resourceSpans()).singleElement().satisfies(rs -> {
            assertThat(rs.resource()).isEqualTo(resource);
            assertThat(rs.scopeSpans()).singleElement().satisfies(ss ->
                    assertThat(ss.scope()).isEqualTo(scope));
        });
        assertThat(data.spans()).extracting(Span::name).containsExactly("op");
    }

    @DisplayName("MetricsData.of wraps metrics under one resource and scope")
    @Test
    void metricsDataOfWrapsMetricsUnderOneResourceAndScope() {
        var metric = Metric.builder().name("m").data(new Metric.Gauge(List.of())).build();

        var data = MetricsData.of(Resource.EMPTY, InstrumentationScope.of("lib"), List.of(metric));

        assertThat(data.metrics()).extracting(Metric::name).containsExactly("m");
    }

    @DisplayName("LogsData.of wraps records under one resource and scope")
    @Test
    void logsDataOfWrapsRecordsUnderOneResourceAndScope() {
        var record = LogRecord.builder().eventName("evt").build();

        var data = LogsData.of(Resource.EMPTY, InstrumentationScope.of("lib"), List.of(record));

        assertThat(data.logRecords()).extracting(LogRecord::eventName).containsExactly("evt");
    }

    @DisplayName("ProfilesData.of wraps profiles under one resource and scope")
    @Test
    void profilesDataOfWrapsProfilesUnderOneResourceAndScope() {
        var profile = new ProfilesData.Profile("p", 0L, 0L, 0L, 0, 0, "", new byte[0]);

        var data = ProfilesData.of(Resource.EMPTY, InstrumentationScope.of("lib"), List.of(profile));

        assertThat(data.profiles()).extracting(ProfilesData.Profile::profileId).containsExactly("p");
    }
}
