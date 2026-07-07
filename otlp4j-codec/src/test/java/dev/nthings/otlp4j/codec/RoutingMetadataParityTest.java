package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins the agreement between the two payload supplies of the [PayloadView#ROUTING_METADATA] view: for
/// the same telemetry a [ProtoRequestPayload] and a [DomainRequestPayload] answer the routing surface
/// identically, and both merge the resources of a multi-resource batch so a routing question is
/// answered once.
///
/// Every fixture here carries two resources whose attribute sets differ, so a supply that read only the
/// first resource, or that flattened the resources incorrectly, would answer `service.name` or the key
/// set differently across the two payloads and fail here rather than pass by accident on a
/// single-resource batch.
@DisplayName("Routing metadata parity")
class RoutingMetadataParityTest {

    // First resource in wire order: service.name=checkout, tenant=acme.
    private static final Resource RESOURCE_A =
            Resource.of(Attributes.builder().put("service.name", "checkout").put("tenant", "acme").build());
    // Second resource: a distinct service.name (so first-match-wins is observable) plus a new key.
    private static final Resource RESOURCE_B = Resource.of(
            Attributes.builder().put("service.name", "payments").put("deployment.environment", "prod").build());

    @DisplayName("traces routing metadata agrees across proto-backed and domain-backed payloads")
    @Test
    void traces() {
        var scope = Fixtures.testScope();
        var span = Fixtures.span("s", Span.Kind.SERVER);
        var batch = new TracesData(List.of(
                new TracesData.ResourceSpans(
                        RESOURCE_A, "", List.of(new TracesData.ScopeSpans(scope, "", List.of(span)))),
                new TracesData.ResourceSpans(
                        RESOURCE_B, "", List.of(new TracesData.ScopeSpans(scope, "", List.of(span))))));
        assertParity(SignalCodec.TRACES, batch, 2L);
    }

    @DisplayName("metrics routing metadata agrees across proto-backed and domain-backed payloads")
    @Test
    void metrics() {
        var scope = Fixtures.testScope();
        var metric = Fixtures.metric("m");
        var batch = new MetricsData(List.of(
                new MetricsData.ResourceMetrics(
                        RESOURCE_A, "", List.of(new MetricsData.ScopeMetrics(scope, "", List.of(metric)))),
                new MetricsData.ResourceMetrics(
                        RESOURCE_B, "", List.of(new MetricsData.ScopeMetrics(scope, "", List.of(metric))))));
        assertParity(SignalCodec.METRICS, batch, 2L);
    }

    @DisplayName("logs routing metadata agrees across proto-backed and domain-backed payloads")
    @Test
    void logs() {
        var scope = Fixtures.testScope();
        var record = Fixtures.logRecord("body", LogRecord.Severity.INFO);
        var batch = new LogsData(List.of(
                new LogsData.ResourceLogs(
                        RESOURCE_A, "", List.of(new LogsData.ScopeLogs(scope, "", List.of(record)))),
                new LogsData.ResourceLogs(
                        RESOURCE_B, "", List.of(new LogsData.ScopeLogs(scope, "", List.of(record))))));
        assertParity(SignalCodec.LOGS, batch, 2L);
    }

    @DisplayName("profiles routing metadata agrees across proto-backed and domain-backed payloads")
    @Test
    void profiles() {
        var scope = Fixtures.testScope();
        var profile = Fixtures.profile("0102030405060708");
        var batch = new ProfilesData(
                List.of(
                        new ProfilesData.ResourceProfiles(
                                RESOURCE_A, "", List.of(new ProfilesData.ScopeProfiles(scope, "", List.of(profile)))),
                        new ProfilesData.ResourceProfiles(
                                RESOURCE_B, "", List.of(new ProfilesData.ScopeProfiles(scope, "", List.of(profile))))),
                new byte[0]);
        assertParity(SignalCodec.PROFILES, batch, 2L);
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertParity(
            SignalCodec<T, REQ, RESP> codec, T batch, long expectedItems) {
        var proto = new ProtoRequestPayload<>(codec, codec.toProto(batch));
        var domain = new DomainRequestPayload<>(codec, batch);

        RoutingMetadata protoMeta = proto.view(PayloadView.ROUTING_METADATA);
        RoutingMetadata domainMeta = domain.view(PayloadView.ROUTING_METADATA);

        for (var meta : List.of(protoMeta, domainMeta)) {
            assertThat(meta.signal()).as("signal").isSameAs(codec.descriptor());
            assertThat(meta.itemCount()).as("item count").isEqualTo(expectedItems);
            assertThat(meta.resourceAttribute("service.name"))
                    .as("first-match-wins across resources")
                    .contains("checkout");
            assertThat(meta.resourceAttribute("tenant")).as("first resource attribute").contains("acme");
            assertThat(meta.resourceAttribute("deployment.environment"))
                    .as("second resource attribute")
                    .contains("prod");
            assertThat(meta.resourceAttribute("absent")).as("absent key").isEmpty();
            assertThat(meta.resourceAttributeKeys())
                    .as("merged key set")
                    .containsExactlyInAnyOrder("service.name", "tenant", "deployment.environment");
        }

        assertThat(protoMeta.resourceAttribute("service.name"))
                .as("proto and domain service.name agree")
                .isEqualTo(domainMeta.resourceAttribute("service.name"));
        assertThat(protoMeta.resourceAttributeKeys())
                .as("proto and domain key sets agree")
                .isEqualTo(domainMeta.resourceAttributeKeys());
        assertThat(protoMeta.itemCount())
                .as("proto and domain item counts agree")
                .isEqualTo(domainMeta.itemCount());
    }
}
