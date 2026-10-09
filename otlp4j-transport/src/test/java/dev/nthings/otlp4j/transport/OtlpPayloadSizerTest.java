package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("OTLP payload sizing")
class OtlpPayloadSizerTest {

    @DisplayName("the exact sizer weighs any payload at its request size; the default weighs only one with a wire form")
    @Test
    void exactSizerWeighsAnyPayloadWhileTheDefaultWeighsOnlyAPayloadWithAWireForm() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER), Fixtures.span("b", Span.Kind.CLIENT));
        var request = SignalCodec.TRACES.toProto(batch);
        var inProcess = Payload.of(TRACES, batch);
        var received = SignalCodec.TRACES.proto(request);
        // A received payload whose batch a transform replaced: it keeps a wire form of its own.
        var replaced = received.withData(Fixtures.traceData(Fixtures.span("c", Span.Kind.SERVER)));
        var replacedSize = SignalCodec.TRACES.toProto(replaced.data()).getSerializedSize();

        var exact = OtlpPayloadSizer.exact();
        assertThat(exact.sizeOf(inProcess)).isEqualTo(request.getSerializedSize());
        assertThat(exact.sizeOf(received)).isEqualTo(request.getSerializedSize());
        assertThat(exact.sizeOf(replaced)).isEqualTo(replacedSize);

        var reported = PayloadSizer.reported();
        assertThat(reported.sizeOf(inProcess)).isZero();
        assertThat(reported.sizeOf(received)).isEqualTo(request.getSerializedSize());
        assertThat(reported.sizeOf(replaced)).isEqualTo(replacedSize);
    }
}
