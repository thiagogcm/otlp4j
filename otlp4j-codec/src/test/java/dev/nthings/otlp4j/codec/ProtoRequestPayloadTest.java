package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit contract for [ProtoRequestPayload]: it answers from the carried proto request without
/// materialising the domain, and materialises the domain only when the immutable view is requested.
@DisplayName("ProtoRequestPayload")
class ProtoRequestPayloadTest {

    private static ProtoRequestPayload<
            dev.nthings.otlp4j.model.TracesData,
            io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest,
            io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse> tracePayload() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", dev.nthings.otlp4j.model.Span.Kind.SERVER),
                Fixtures.span("b", dev.nthings.otlp4j.model.Span.Kind.CLIENT));
        var request = SignalCodec.TRACES.toProto(batch);
        return new ProtoRequestPayload<>(SignalCodec.TRACES, request);
    }

    @DisplayName("signal, item count, and size answer from the proto request")
    @Test
    void answersFromProto() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", dev.nthings.otlp4j.model.Span.Kind.SERVER),
                Fixtures.span("b", dev.nthings.otlp4j.model.Span.Kind.CLIENT));
        var request = SignalCodec.TRACES.toProto(batch);
        var payload = new ProtoRequestPayload<>(SignalCodec.TRACES, request);

        assertThat(payload.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(payload.itemCount()).isEqualTo(SignalCodec.TRACES.protoItemCount(request));
        assertThat(payload.itemCount()).isEqualTo(2L);
        assertThat(payload.estimatedSerializedSize()).isEqualTo(request.getSerializedSize());
    }

    @DisplayName("PROTO_REQUEST is the carried message; DOMAIN_IMMUTABLE decodes it")
    @Test
    void views() {
        var batch = Fixtures.traceData(Fixtures.span("a", dev.nthings.otlp4j.model.Span.Kind.SERVER));
        var request = SignalCodec.TRACES.toProto(batch);
        var payload = new ProtoRequestPayload<>(SignalCodec.TRACES, request);

        assertThat(payload.view(PayloadView.PROTO_REQUEST)).isSameAs(request);
        assertThat(payload.view(PayloadView.DOMAIN_IMMUTABLE)).isEqualTo(SignalCodec.TRACES.toDomain(request));
    }

    @DisplayName("writeTo serialises the carried message")
    @Test
    void writesProtoBytes() throws IOException {
        var batch = Fixtures.traceData(Fixtures.span("a", dev.nthings.otlp4j.model.Span.Kind.SERVER));
        var request = SignalCodec.TRACES.toProto(batch);
        var payload = new ProtoRequestPayload<>(SignalCodec.TRACES, request);

        var out = new ByteArrayOutputStream();
        payload.writeTo(out);
        assertThat(out.toByteArray()).isEqualTo(request.toByteArray());
    }

    @DisplayName("copyForMutation shares the immutable backing")
    @Test
    void copyShares() {
        var payload = tracePayload();
        assertThat(payload.copyForMutation()).isSameAs(payload);
    }

    @DisplayName("an unknown view fails closed")
    @Test
    void unknownViewThrows() {
        var payload = tracePayload();
        assertThatThrownBy(() -> payload.view(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("null request and null codec are rejected")
    @Test
    void rejectsNulls() {
        assertThatThrownBy(() -> new ProtoRequestPayload<>(SignalCodec.TRACES, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProtoRequestPayload<>(
                null, SignalCodec.TRACES.toProto(Fixtures.traceData())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("a pass-through export never materialises the domain until it is viewed")
    @Test
    void passThroughDoesNotMaterialiseDomain() throws IOException {
        var payload = tracePayload();

        payload.signal();
        payload.itemCount();
        payload.estimatedSerializedSize();
        payload.view(PayloadView.PROTO_REQUEST);
        payload.writeTo(new ByteArrayOutputStream());

        assertThat(payload.domainMaterialised()).isFalse();

        payload.view(PayloadView.DOMAIN_IMMUTABLE);
        assertThat(payload.domainMaterialised()).isTrue();
    }
}
