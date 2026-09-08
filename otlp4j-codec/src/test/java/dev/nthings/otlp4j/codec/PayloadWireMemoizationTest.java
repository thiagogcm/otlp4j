package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.UnknownFieldSet;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests the internal wire-boundary memoization used by in-process transport paths.
@DisplayName("PayloadWire memoization")
class PayloadWireMemoizationTest {

    @Test
    @DisplayName("proto payload identity and unknown fields are preserved")
    void preservesProtoPassThrough() {
        var unknowns = UnknownFieldSet.newBuilder()
                .mergeField(999, UnknownFieldSet.Field.newBuilder().addVarint(42).build())
                .build();
        var request = SignalCodec.TRACES.toProto(Fixtures.traceData()).toBuilder()
                .setUnknownFields(unknowns)
                .build();
        Payload<TracesData> payload = PayloadWire.proto(SignalCodec.TRACES, request);

        assertThat(PayloadWire.memoize(payload)).isSameAs(payload);
        var memoizedRequest = PayloadWire.request(SignalCodec.TRACES, PayloadWire.memoize(payload));
        assertThat(memoizedRequest).isSameAs(request);
        assertThat(memoizedRequest.getUnknownFields()).isEqualTo(unknowns);
    }

    @Test
    @DisplayName("an arbitrary domain payload is read once before size and request")
    void memoizesArbitraryDomainPayload() {
        var batch = Fixtures.traceData();
        var dataReads = new AtomicInteger();
        Payload<TracesData> source = new Payload<>() {
            @Override
            public SignalDescriptor<TracesData> signal() {
                return SignalDescriptor.TRACES;
            }

            @Override
            public TracesData data() {
                dataReads.incrementAndGet();
                return batch;
            }

            @Override
            public long itemCount() {
                return batch.spanCount();
            }

            @Override
            public RoutingMetadata routingMetadata() {
                return new RoutingMetadata() {
                    @Override
                    public Optional<String> resourceAttribute(String key) {
                        return Optional.empty();
                    }

                    @Override
                    public Set<String> resourceAttributeKeys() {
                        return Set.of();
                    }
                };
            }

            @Override
            public Payload<TracesData> withData(TracesData replacement) {
                return this;
            }
        };

        var memoized = PayloadWire.memoize(source);
        var expected = SignalCodec.TRACES.toProto(batch);

        assertThat(dataReads).hasValue(1);
        assertThat(PayloadWire.size(memoized)).isEqualTo(expected.getSerializedSize());
        assertThat(PayloadWire.request(SignalCodec.TRACES, memoized)).isSameAs(
                PayloadWire.request(SignalCodec.TRACES, memoized));
        assertThat(dataReads).hasValue(1);
    }
}
