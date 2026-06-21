package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Mapper-unit round-trip coverage for [TraceMapper]: spans with events/links/status present
/// and absent, an empty `TraceData`, and a multi-resource / multi-scope payload.
@DisplayName("TraceMapper round-trips")
class TraceMapperRoundTripTest {

    private static Span bareSpan() {
        return Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("bare")
                .kind(Span.Kind.INTERNAL)
                .build();
    }

    private static Span spanWithEventsAndLinks() {
        return Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .parentSpanId("1112131415161718")
                .traceState("vendor=abc")
                .flags(1L)
                .name("full")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                .attributes(Attributes.builder().put("http.method", "GET").build())
                .droppedAttributesCount(3)
                .addEvent(new Span.Event(
                        1_500L, "evt", Attributes.builder().put("k", "v").build(), 1))
                .droppedEventsCount(2)
                .addLink(new Span.Link(
                        "aabbccddeeff00112233445566778899",
                        "aabbccddeeff0011",
                        "vendor=xyz",
                        Attributes.builder().put("link.attr", true).build(),
                        4,
                        2L))
                .droppedLinksCount(5)
                .status(new Span.Status(Span.Status.Code.ERROR, "boom"))
                .build();
    }

    static Stream<Span> spanVariants() {
        return Stream.of(
                bareSpan(),
                spanWithEventsAndLinks(),
                // status present but OK with empty message
                Span.builder()
                        .traceId("0102030405060708090a0b0c0d0e0f10")
                        .spanId("0102030405060708")
                        .name("ok-status")
                        .kind(Span.Kind.CLIENT)
                        .status(new Span.Status(Span.Status.Code.OK, ""))
                        .build());
    }

    @DisplayName("Spans with and without events, links and status round-trip through TraceMapper")
    @ParameterizedTest
    @MethodSource("spanVariants")
    void roundTripsSpanVariants(Span span) {
        var sent = Fixtures.traceData(span);
        assertThat(TraceMapper.toDomain(TraceMapper.toProto(sent)))
                .as("toDomain(toProto(x)) must equal x for spans with/without events, links, status")
                .isEqualTo(sent);
    }

    @DisplayName("Empty TraceData round-trips through TraceMapper")
    @Test
    void roundTripsAnEmptyTraceData() {
        var sent = new TraceData(List.of());
        assertThat(TraceMapper.toDomain(TraceMapper.toProto(sent))).isEqualTo(sent);
    }

    @DisplayName("Multi-resource and multi-scope trace payloads round-trip through TraceMapper")
    @Test
    void roundTripsMultipleResourcesAndScopes() {
        var sent = TransportFixtures.traceWithScopes(3);
        assertThat(TraceMapper.toDomain(TraceMapper.toProto(sent)))
                .as("multi-resource / multi-scope trace payloads must round-trip intact")
                .isEqualTo(sent);
    }
}
