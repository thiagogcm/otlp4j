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

/// Domain payloads preserve telemetry when exported or replaced.
@DisplayName("Domain-backed payload")
class DomainRequestPayloadTest {

    private static DomainRequestPayload<
            TracesData,
            ExportTraceServiceRequest,
            ExportTraceServiceResponse> tracePayload(TracesData batch) {
        return new DomainRequestPayload<>(SignalCodec.TRACES, batch);
    }

    @Test
    @DisplayName("typed domain operations answer without encoding")
    void answersFromDomain() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER), Fixtures.span("b", Span.Kind.CLIENT));
        var payload = tracePayload(batch);

        assertThat(payload.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(payload.data()).isEqualTo(batch);
        assertThat(payload.itemCount()).isEqualTo(2L);
        assertThat(payload.routingMetadata().resourceAttribute("service.name")).contains("checkout");
    }

    @Test
    @DisplayName("wire sizing and writing use the qualified codec boundary")
    void sizeAndWrite() throws IOException {
        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        Payload<TracesData> payload = tracePayload(batch);
        var request = SignalCodec.TRACES.toProto(batch);

        assertThat(PayloadWire.size(payload)).isEqualTo(request.getSerializedSize());

        var out = new ByteArrayOutputStream();
        PayloadWire.write(SignalCodec.TRACES, payload, out);
        assertThat(out.toByteArray()).isEqualTo(request.toByteArray());
    }

    @Test
    @DisplayName("replacement exports new telemetry and leaves the original payload intact")
    void replacementExportsNewData() {
        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var payload = tracePayload(batch);
        var replacement = Fixtures.traceData(Fixtures.span("b", Span.Kind.CLIENT));
        var replaced = payload.withData(replacement);
        assertThat(PayloadWire.request(SignalCodec.TRACES, replaced)
                .getResourceSpans(0).getScopeSpans(0).getSpans(0).getName()).isEqualTo("b");
        assertThat(PayloadWire.request(SignalCodec.TRACES, payload)
                .getResourceSpans(0).getScopeSpans(0).getSpans(0).getName()).isEqualTo("a");
    }

    @Test
    @DisplayName("null batch and null codec are rejected")
    void rejectsNulls() {
        assertThatThrownBy(() -> new DomainRequestPayload<>(SignalCodec.TRACES, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DomainRequestPayload<>(null, Fixtures.traceData()))
                .isInstanceOf(NullPointerException.class);
    }
}
