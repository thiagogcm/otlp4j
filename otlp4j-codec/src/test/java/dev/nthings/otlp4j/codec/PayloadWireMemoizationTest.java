package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.UnknownFieldSet;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Forwarding preserves wire extensions alongside modeled telemetry.
@DisplayName("Payload wire passthrough")
class PayloadWireMemoizationTest {

    @Test
    @DisplayName("proto spans and unknown fields are preserved")
    void preservesProtoPassThrough() {
        var request = requestWithUnknownField();
        Payload<TracesData> payload = PayloadWire.proto(SignalCodec.TRACES, request);

        var memoizedRequest = PayloadWire.request(SignalCodec.TRACES, PayloadWire.memoize(payload));
        assertThat(memoizedRequest.toByteArray()).isEqualTo(request.toByteArray());
    }

    @Test
    @DisplayName("an item-level transform forwards the received request unless it changes an item")
    void itemTransformKeepsTheReceivedRequestUnlessItChangesAnItem() {
        var request = requestWithUnknownField();
        Payload<TracesData> payload = PayloadWire.proto(SignalCodec.TRACES, request);

        var untouched = payload.withData(payload.data().mapSpans(span -> rename(span, "absent", "renamed")));
        assertThat(PayloadWire.request(SignalCodec.TRACES, untouched)).isSameAs(request);

        var renamed = payload.withData(payload.data().mapSpans(span -> rename(span, "checkout", "renamed")));
        var encoded = PayloadWire.request(SignalCodec.TRACES, renamed);
        assertThat(encoded.getResourceSpans(0).getScopeSpans(0).getSpans(0).getName()).isEqualTo("renamed");
        assertThat(encoded.getUnknownFields().asMap()).isEmpty();
    }

    private static ExportTraceServiceRequest requestWithUnknownField() {
        var unknowns = UnknownFieldSet.newBuilder()
                .mergeField(999, UnknownFieldSet.Field.newBuilder().addVarint(42).build())
                .build();
        return SignalCodec.TRACES.toProto(Fixtures.traceData(Fixtures.span("checkout", Span.Kind.SERVER))).toBuilder()
                .setUnknownFields(unknowns)
                .build();
    }

    private static Span rename(Span span, String from, String to) {
        return span.name().equals(from) ? span.toBuilder().name(to).build() : span;
    }
}
