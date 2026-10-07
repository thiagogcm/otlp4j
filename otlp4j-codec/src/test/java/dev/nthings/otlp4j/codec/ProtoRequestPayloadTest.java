package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    @DisplayName("null request and null codec are rejected")
    void rejectsNulls() {
        assertThatThrownBy(() -> new ProtoRequestPayload<>(SignalCodec.TRACES, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ProtoRequestPayload<>(null, SignalCodec.TRACES.toProto(Fixtures.traceData())))
                .isInstanceOf(NullPointerException.class);
    }
}
