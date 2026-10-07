package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.UnknownFieldSet;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Forwarding preserves wire extensions alongside modeled telemetry.
@DisplayName("Payload wire passthrough")
class PayloadWireMemoizationTest {

    @Test
    @DisplayName("proto spans and unknown fields are preserved")
    void preservesProtoPassThrough() {
        var unknowns = UnknownFieldSet.newBuilder()
                .mergeField(999, UnknownFieldSet.Field.newBuilder().addVarint(42).build())
                .build();
        var request = SignalCodec.TRACES.toProto(Fixtures.traceData(Fixtures.span("checkout", Span.Kind.SERVER))).toBuilder()
                .setUnknownFields(unknowns)
                .build();
        Payload<TracesData> payload = PayloadWire.proto(SignalCodec.TRACES, request);

        var memoizedRequest = PayloadWire.request(SignalCodec.TRACES, PayloadWire.memoize(payload));
        assertThat(memoizedRequest.toByteArray()).isEqualTo(request.toByteArray());
    }
}
