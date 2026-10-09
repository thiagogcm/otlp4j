package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Protobuf-backed forwarding and routing stay pass-through until a
/// typed domain operation is requested.
@DisplayName("Proto-backed payload")
class ProtoRequestPayloadTest {

    private static ProtoRequestPayload<
            TracesData,
            ExportTraceServiceRequest,
            ExportTraceServiceResponse> tracePayload() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER), Fixtures.span("b", Span.Kind.CLIENT));
        return new ProtoRequestPayload<>(SignalCodec.TRACES, SignalCodec.TRACES.toProto(batch));
    }

    @Test
    @DisplayName("signal, count, size, and request answer from the carried proto")
    void answersFromProto() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER), Fixtures.span("b", Span.Kind.CLIENT));
        var request = SignalCodec.TRACES.toProto(batch);
        var payload = new ProtoRequestPayload<>(SignalCodec.TRACES, request);

        assertThat(payload.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(payload.itemCount()).isEqualTo(2L);
        assertThat(PayloadWire.size(payload)).isEqualTo(request.getSerializedSize());
        assertThat(PayloadWire.request(SignalCodec.TRACES, payload).toByteArray()).isEqualTo(request.toByteArray());
    }

    @Test
    @DisplayName("wire writing preserves the carried request")
    void writesProtoBytes() throws IOException {
        var payload = tracePayload();
        var out = new ByteArrayOutputStream();
        PayloadWire.write(SignalCodec.TRACES, payload, out);
        assertThat(out.toByteArray()).isEqualTo(PayloadWire.request(SignalCodec.TRACES, payload).toByteArray());
        assertThat(payload.dataMaterialized()).isFalse();
    }

    @Test
    @DisplayName("data decodes lazily and withData creates a domain-backed replacement")
    void decodesLazily() {
        var payload = tracePayload();
        assertThat(payload.dataMaterialized()).isFalse();
        assertThat(payload.data().spans()).extracting(Span::name).containsExactly("a", "b");
        assertThat(payload.dataMaterialized()).isTrue();
        var replacement = Fixtures.traceData(Fixtures.span("c", Span.Kind.CLIENT));
        Payload<TracesData> replaced = payload.withData(replacement);
        assertThat(PayloadWire.request(SignalCodec.TRACES, replaced)
                .getResourceSpans(0).getScopeSpans(0).getSpans(0).getName()).isEqualTo("c");
        assertThat(payload.data().spans()).extracting(Span::name).containsExactly("a", "b");
    }

    @Test
    @DisplayName("dropping a resource group re-encodes only the kept group and keeping all keeps the request")
    void droppingAResourceGroupReencodesOnlyTheKeptGroup() {
        var batch = new TracesData(List.of(resourceSpans("checkout", "a"), resourceSpans("billing", "b")));
        var request = SignalCodec.TRACES.toProto(batch);
        var payload = new ProtoRequestPayload<>(SignalCodec.TRACES, request);

        var kept = payload.withData(
                SignalDescriptor.TRACES.filterResources(payload.data(), ProtoRequestPayloadTest::isBilling));

        var encoded = PayloadWire.request(SignalCodec.TRACES, kept);
        assertThat(encoded.getResourceSpansCount()).isEqualTo(1);
        assertThat(encoded.getResourceSpans(0).toByteArray()).isEqualTo(request.getResourceSpans(1).toByteArray());
        assertThat(kept.itemCount()).isEqualTo(1L);
        assertThat(kept.routingMetadata().resources())
                .containsExactly(payload.routingMetadata().resources().get(1));

        var all = payload.withData(SignalDescriptor.TRACES.filterResources(payload.data(), resource -> true));
        assertThat(all).isSameAs(payload);
        assertThat(PayloadWire.request(SignalCodec.TRACES, all)).isSameAs(request);
    }

    @Test
    @DisplayName("dropping a profiles resource group keeps the shared dictionary and the kept group's bytes")
    void droppingAProfilesResourceGroupKeepsTheDictionary() {
        var dictionary = ProfilesDictionary.newBuilder().addStringTable("").addStringTable("cpu").build();
        var batch = new ProfilesData(
                List.of(resourceProfiles("checkout", "0102030405060708"), resourceProfiles("billing", "1112131415161718")),
                dictionary.toByteArray());
        var request = SignalCodec.PROFILES.toProto(batch);
        var payload = new ProtoRequestPayload<>(SignalCodec.PROFILES, request);

        var kept = payload.withData(
                SignalDescriptor.PROFILES.filterResources(payload.data(), ProtoRequestPayloadTest::isBilling));

        var encoded = PayloadWire.request(SignalCodec.PROFILES, kept);
        assertThat(encoded.getDictionary().toByteArray()).isEqualTo(dictionary.toByteArray());
        assertThat(encoded.getResourceProfilesCount()).isEqualTo(1);
        assertThat(encoded.getResourceProfiles(0).toByteArray())
                .isEqualTo(request.getResourceProfiles(1).toByteArray());
    }

    private static boolean isBilling(Resource resource) {
        return "billing".equals(resource.attributes().getString("service.name"));
    }

    private static Resource service(String name) {
        return Resource.of(Attributes.builder().put("service.name", name).build());
    }

    private static TracesData.ResourceSpans resourceSpans(String service, String spanName) {
        return new TracesData.ResourceSpans(service(service), service + "-url", List.of(
                new TracesData.ScopeSpans(Fixtures.testScope(), "", List.of(Fixtures.span(spanName, Span.Kind.SERVER)))));
    }

    private static ProfilesData.ResourceProfiles resourceProfiles(String service, String profileId) {
        return new ProfilesData.ResourceProfiles(service(service), "", List.of(
                new ProfilesData.ScopeProfiles(Fixtures.testScope(), "", List.of(Fixtures.profile(profileId)))));
    }

    @Test
    @DisplayName("null request and null codec are rejected")
    void rejectsNulls() {
        assertThatThrownBy(() -> new ProtoRequestPayload<>(SignalCodec.TRACES, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ProtoRequestPayload<>(null, SignalCodec.TRACES.toProto(Fixtures.traceData())))
                .isInstanceOf(NullPointerException.class);
    }
}
