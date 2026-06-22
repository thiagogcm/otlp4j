package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Pins construction-time validation and the explicit wire-number mapping that move failures off
/// the asynchronous export thread and decouple enum encoding from declaration order.
@DisplayName("Model construction validation")
class ConstructionValidationTest {

    // --- null validation at construction (not later, off-thread) ----------------------------

    @DisplayName("Resource rejects null attributes")
    @Test
    void resourceRejectsNullAttributes() {
        assertThatThrownBy(() -> new Resource(null, 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("attributes");
    }

    @DisplayName("InstrumentationScope rejects null name, version, or attributes")
    @Test
    void instrumentationScopeRejectsNulls() {
        assertThatThrownBy(() -> new InstrumentationScope(null, "v", Attributes.empty(), 0))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("name");
        assertThatThrownBy(() -> new InstrumentationScope("n", null, Attributes.empty(), 0))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("version");
        assertThatThrownBy(() -> new InstrumentationScope("n", "v", null, 0))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("attributes");
    }

    @DisplayName("Attributes.of rejects a null map with a named NPE")
    @Test
    void attributesOfRejectsNull() {
        assertThatThrownBy(() -> Attributes.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("values");
    }

    @DisplayName("Span rejects null identifiers and required references at construction")
    @Test
    void spanRejectsNullReferences() {
        assertThatThrownBy(() -> Span.builder().traceId(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("traceId");
        assertThatThrownBy(() -> Span.builder().spanId(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("spanId");
        assertThatThrownBy(() -> Span.builder().kind(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("kind");
        assertThatThrownBy(() -> Span.builder().status(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("status");
    }

    @DisplayName("Span.Builder.events/links reject null before mutating the builder")
    @Test
    void spanBuilderRejectsNullEventsAndLinks() {
        var builder = Span.builder().addEvent(new Span.Event(1L, "kept", Attributes.empty(), 0));
        assertThatThrownBy(() -> builder.events(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("events");
        assertThatThrownBy(() -> builder.links(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("links");
        // The failed setters must not have cleared the previously added event.
        assertThat(builder.build().events()).hasSize(1);
    }

    // --- AttributeValue defensive copies in the canonical constructor -----------------------

    @DisplayName("ArrayValue and KeyValueListValue copy via their canonical constructors")
    @Test
    void attributeValueCollectionsAreCopiedAtConstruction() {
        var listSource = new ArrayList<AttributeValue>(List.of(AttributeValue.of("a")));
        var array = new AttributeValue.ArrayValue(listSource);
        listSource.add(AttributeValue.of("late"));
        assertThat(array.values()).hasSize(1);
        assertThatThrownBy(() -> array.values().add(AttributeValue.of("x")))
                .isInstanceOf(UnsupportedOperationException.class);

        var mapSource = new LinkedHashMap<String, AttributeValue>();
        mapSource.put("k", AttributeValue.of("v"));
        var map = new AttributeValue.KeyValueListValue(mapSource);
        mapSource.put("late", AttributeValue.of("v2"));
        assertThat(map.values()).hasSize(1);
        assertThatThrownBy(() -> map.values().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- explicit wire numbers (decoupled from declaration order) ---------------------------

    @DisplayName("Every Span.Kind maps to its number and back, unknown numbers degrade to UNSPECIFIED")
    @ParameterizedTest
    @EnumSource(Span.Kind.class)
    void spanKindNumberRoundTrips(Span.Kind kind) {
        assertThat(Span.Kind.fromNumber(kind.number())).isEqualTo(kind);
        assertThat(Span.Kind.fromNumber(999)).isEqualTo(Span.Kind.UNSPECIFIED);
    }

    @DisplayName("Every Span.Status.Code maps to its number and back, unknown numbers degrade to UNSET")
    @ParameterizedTest
    @EnumSource(Span.Status.Code.class)
    void statusCodeNumberRoundTrips(Span.Status.Code code) {
        assertThat(Span.Status.Code.fromNumber(code.number())).isEqualTo(code);
        assertThat(Span.Status.Code.fromNumber(-1)).isEqualTo(Span.Status.Code.UNSET);
    }

    @DisplayName("Every AggregationTemporality maps to its number and back, unknown numbers degrade")
    @ParameterizedTest
    @EnumSource(Metric.AggregationTemporality.class)
    void temporalityNumberRoundTrips(Metric.AggregationTemporality temporality) {
        assertThat(Metric.AggregationTemporality.fromNumber(temporality.number())).isEqualTo(temporality);
        assertThat(Metric.AggregationTemporality.fromNumber(42))
                .isEqualTo(Metric.AggregationTemporality.UNSPECIFIED);
    }
}
