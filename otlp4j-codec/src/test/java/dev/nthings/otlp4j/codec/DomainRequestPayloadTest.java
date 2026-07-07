package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.PayloadMemoryMode;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit contract for [DomainRequestPayload]: it answers from the carried domain batch without
/// materialising the proto request, and materialises the proto request only when it is sized,
/// serialised, or the proto view is requested.
@DisplayName("DomainRequestPayload")
class DomainRequestPayloadTest {

    private static DomainRequestPayload<
            TracesData,
            io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest,
            io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse> tracePayload(TracesData batch) {
        return new DomainRequestPayload<>(SignalCodec.TRACES, batch);
    }

    @DisplayName("signal, item count, and batch answer from the domain batch")
    @Test
    void answersFromDomain() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER), Fixtures.span("b", Span.Kind.CLIENT));
        var payload = tracePayload(batch);

        assertThat(payload.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(payload.itemCount()).isEqualTo(SignalDescriptor.TRACES.itemCount(batch));
        assertThat(payload.itemCount()).isEqualTo(2L);
        assertThat(payload.batch()).isSameAs(batch);
        assertThat(payload.memoryMode()).isEqualTo(PayloadMemoryMode.DOMAIN_BACKED);
    }

    @DisplayName("DOMAIN_IMMUTABLE is the carried batch; PROTO_REQUEST encodes it")
    @Test
    void views() {
        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var payload = tracePayload(batch);

        assertThat(payload.view(PayloadView.DOMAIN_IMMUTABLE)).isSameAs(batch);
        assertThat(payload.view(PayloadView.PROTO_REQUEST)).isEqualTo(SignalCodec.TRACES.toProto(batch));
    }

    @DisplayName("size and writeTo materialise and serialise the proto request")
    @Test
    void sizeAndWrite() throws IOException {
        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var payload = tracePayload(batch);
        var request = SignalCodec.TRACES.toProto(batch);

        assertThat(payload.estimatedSerializedSize()).isEqualTo(request.getSerializedSize());

        var out = new ByteArrayOutputStream();
        payload.writeTo(out);
        assertThat(out.toByteArray()).isEqualTo(request.toByteArray());
    }

    @DisplayName("copyForMutation shares the immutable backing")
    @Test
    void copyShares() {
        var payload = tracePayload(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)));
        assertThat(payload.copyForMutation()).isSameAs(payload);
    }

    @DisplayName("an unknown view fails closed")
    @Test
    void unknownViewThrows() {
        var payload = tracePayload(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)));
        assertThatThrownBy(() -> payload.view(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("null batch and null codec are rejected")
    @Test
    void rejectsNulls() {
        assertThatThrownBy(() -> new DomainRequestPayload<>(SignalCodec.TRACES, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DomainRequestPayload<>(null, Fixtures.traceData()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
