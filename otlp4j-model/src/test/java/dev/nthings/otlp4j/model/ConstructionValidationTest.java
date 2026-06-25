package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.OptionalDouble;
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

    @DisplayName("Span rejects ids of the wrong length or with non-hex characters at construction")
    @Test
    void spanRejectsMalformedIdentifiers() {
        assertThatThrownBy(() -> Span.builder().traceId("abc").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("traceId");
        assertThatThrownBy(() -> Span.builder().spanId("def").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("spanId");
        // 32 chars but not all hex.
        assertThatThrownBy(() -> Span.builder().traceId("zz020304050607080910111213141516").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("traceId");
        // Empty ids are valid (absent) and uppercase hex is accepted.
        assertThat(Span.builder().traceId("0102030405060708090A0B0C0D0E0F10").build().traceId())
                .isEqualTo("0102030405060708090A0B0C0D0E0F10");
    }

    @DisplayName("Span and Span.Link reject flags outside the unsigned 32-bit range at construction")
    @Test
    void spanFlagsRejectedOutsideUnsignedIntRange() {
        assertThatThrownBy(() -> Span.builder().flags(0x1_0000_0001L).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flags");
        assertThatThrownBy(() -> Span.builder().flags(-1L).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flags");
        // The maximum unsigned 32-bit value is accepted.
        assertThat(Span.builder().flags(0xFFFFFFFFL).build().flags()).isEqualTo(0xFFFFFFFFL);
    }

    @DisplayName("Metric point records reject flags outside the unsigned 32-bit range at construction")
    @Test
    void metricPointFlagsRejectedOutsideUnsignedIntRange() {
        assertThatThrownBy(() -> new NumberPoint(Attributes.empty(), 0L, 0L, null, 0x1_0000_0001L, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flags");
        assertThatThrownBy(() -> new HistogramPoint(
                Attributes.empty(), 0L, 0L, 0L, OptionalDouble.empty(), List.of(), List.of(),
                OptionalDouble.empty(), OptionalDouble.empty(), -1L, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flags");
        assertThatThrownBy(() -> new ExponentialHistogramPoint(
                Attributes.empty(), 0L, 0L, 0L, OptionalDouble.empty(), 0, 0L,
                ExponentialHistogramPoint.Buckets.EMPTY, ExponentialHistogramPoint.Buckets.EMPTY,
                OptionalDouble.empty(), OptionalDouble.empty(), 0.0, 0x1_0000_0001L, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flags");
        assertThatThrownBy(() -> new SummaryPoint(Attributes.empty(), 0L, 0L, 0L, 0.0, List.of(), -1L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flags");
        // Maximum unsigned 32-bit value is accepted.
        assertThat(new NumberPoint(Attributes.empty(), 0L, 0L, null, 0xFFFFFFFFL, List.of()).flags())
                .isEqualTo(0xFFFFFFFFL);
    }

    @DisplayName("Span.Link and LogRecord validate their identifiers at construction")
    @Test
    void linkAndLogRecordValidateIdentifiers() {
        assertThatThrownBy(() ->
                new Span.Link("tooshort", "0102030405060708", "", Attributes.empty(), 0, 0L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("traceId");
        assertThatThrownBy(() -> LogRecord.builder().spanId("nothex-garbage!!").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("spanId");
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
