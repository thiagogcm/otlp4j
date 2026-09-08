package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Proves routing metadata on pass-through payloads never materialises their domain batch.
@DisplayName("Routing metadata without domain materialisation")
class RoutingMetadataNoMaterialisationTest {

    @Test
    void traces() {
        assertNoMaterialisation(SignalCodec.TRACES, Fixtures.traceData(Fixtures.span("s", Span.Kind.SERVER)));
    }

    @Test
    void metrics() {
        assertNoMaterialisation(SignalCodec.METRICS, Fixtures.metricsData(Fixtures.metric("m")));
    }

    @Test
    void logs() {
        assertNoMaterialisation(
                SignalCodec.LOGS, Fixtures.logsData(Fixtures.logRecord("body", LogRecord.Severity.INFO)));
    }

    @Test
    void profiles() {
        assertNoMaterialisation(SignalCodec.PROFILES, Fixtures.profilesData(Fixtures.profile("0102030405060708")));
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertNoMaterialisation(
            SignalCodec<T, REQ, RESP> codec, T batch) {
        var payload = new ProtoRequestPayload<>(codec, codec.toProto(batch));
        RoutingMetadata metadata = payload.routingMetadata();
        metadata.resourceAttribute("service.name");
        metadata.resourceAttributeKeys();

        assertThat(payload.dataMaterialized()).isFalse();
        assertThat(payload.data()).isNotNull();
        assertThat(payload.dataMaterialized()).isTrue();
    }
}
