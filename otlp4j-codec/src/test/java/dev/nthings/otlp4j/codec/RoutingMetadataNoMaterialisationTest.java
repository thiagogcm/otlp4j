package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Proves the pass-through payload answers the [PayloadView#ROUTING_METADATA] view without ever
/// building the domain batch. Reading the routing surface, and exercising every one of its accessors,
/// must leave [ProtoRequestPayload#domainMaterialised] false; a subsequent [PayloadView#DOMAIN_IMMUTABLE]
/// read is asserted to flip it, so the seam the proof relies on is shown to be live rather than stuck.
@DisplayName("Routing metadata no materialisation")
class RoutingMetadataNoMaterialisationTest {

    @DisplayName("traces proto payload answers routing metadata without materialising the domain")
    @Test
    void traces() {
        assertNoMaterialisation(SignalCodec.TRACES, Fixtures.traceData(Fixtures.span("s", Span.Kind.SERVER)));
    }

    @DisplayName("metrics proto payload answers routing metadata without materialising the domain")
    @Test
    void metrics() {
        assertNoMaterialisation(SignalCodec.METRICS, Fixtures.metricsData(Fixtures.metric("m")));
    }

    @DisplayName("logs proto payload answers routing metadata without materialising the domain")
    @Test
    void logs() {
        assertNoMaterialisation(
                SignalCodec.LOGS, Fixtures.logsData(Fixtures.logRecord("body", LogRecord.Severity.INFO)));
    }

    @DisplayName("profiles proto payload answers routing metadata without materialising the domain")
    @Test
    void profiles() {
        assertNoMaterialisation(SignalCodec.PROFILES, Fixtures.profilesData(Fixtures.profile("0102030405060708")));
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertNoMaterialisation(
            SignalCodec<T, REQ, RESP> codec, T batch) {
        var payload = new ProtoRequestPayload<>(codec, codec.toProto(batch));
        var name = codec.descriptor().signalName();

        RoutingMetadata meta = payload.view(PayloadView.ROUTING_METADATA);
        // Exercise every accessor: none of them may force the domain.
        meta.signal();
        meta.itemCount();
        meta.resourceAttribute("service.name");
        meta.resourceAttributeKeys();

        assertThat(payload.domainMaterialised())
                .as("%s: a routing-metadata read must not materialise the domain", name)
                .isFalse();

        // Prove the seam is live: an actual domain read does flip it.
        payload.view(PayloadView.DOMAIN_IMMUTABLE);
        assertThat(payload.domainMaterialised())
                .as("%s: a domain-immutable read materialises the domain", name)
                .isTrue();
    }
}
