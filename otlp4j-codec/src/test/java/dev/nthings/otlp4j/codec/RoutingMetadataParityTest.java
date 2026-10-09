package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.model.Attributes;
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

/// Resource routing values remain available without decoding the telemetry body.
@DisplayName("Routing metadata parity")
class RoutingMetadataParityTest {

    private static final Resource RESOURCE_A =
            Resource.of(Attributes.builder().put("service.name", "checkout").put("tenant", "acme").build());
    private static final Resource RESOURCE_B = Resource.of(
            Attributes.builder().put("service.name", "payments").put("deployment.environment", "prod").build());

    @Test
    void traces() {
        var scope = Fixtures.testScope();
        var span = Fixtures.span("s", Span.Kind.SERVER);
        var batch = new TracesData(List.of(
                new TracesData.ResourceSpans(RESOURCE_A, "", List.of(new TracesData.ScopeSpans(scope, "", List.of(span)))),
                new TracesData.ResourceSpans(RESOURCE_B, "", List.of(new TracesData.ScopeSpans(scope, "", List.of(span))))));
        assertParity(SignalCodec.TRACES, batch);
    }

    @Test
    void metrics() {
        var scope = Fixtures.testScope();
        var metric = Fixtures.metric("m");
        var batch = new MetricsData(List.of(
                new MetricsData.ResourceMetrics(RESOURCE_A, "", List.of(new MetricsData.ScopeMetrics(scope, "", List.of(metric)))),
                new MetricsData.ResourceMetrics(RESOURCE_B, "", List.of(new MetricsData.ScopeMetrics(scope, "", List.of(metric))))));
        assertParity(SignalCodec.METRICS, batch);
    }

    @Test
    void logs() {
        var scope = Fixtures.testScope();
        var record = Fixtures.logRecord("body", LogRecord.Severity.INFO);
        var batch = new LogsData(List.of(
                new LogsData.ResourceLogs(RESOURCE_A, "", List.of(new LogsData.ScopeLogs(scope, "", List.of(record)))),
                new LogsData.ResourceLogs(RESOURCE_B, "", List.of(new LogsData.ScopeLogs(scope, "", List.of(record))))));
        assertParity(SignalCodec.LOGS, batch);
    }

    @Test
    void profiles() {
        var scope = Fixtures.testScope();
        var profile = Fixtures.profile("0102030405060708");
        var batch = new ProfilesData(List.of(
                new ProfilesData.ResourceProfiles(RESOURCE_A, "", List.of(new ProfilesData.ScopeProfiles(scope, "", List.of(profile)))),
                new ProfilesData.ResourceProfiles(RESOURCE_B, "", List.of(new ProfilesData.ScopeProfiles(scope, "", List.of(profile))))), new byte[0]);
        assertParity(SignalCodec.PROFILES, batch);
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertParity(
            SignalCodec<T, REQ, RESP> codec, T batch) {
        var proto = new ProtoRequestPayload<>(codec, codec.toProto(batch));
        var domain = new DomainRequestPayload<>(codec, batch);
        RoutingMetadata protoMetadata = proto.routingMetadata();
        RoutingMetadata domainMetadata = domain.routingMetadata();

        for (var metadata : List.of(protoMetadata, domainMetadata)) {
            assertThat(metadata.resources()).containsExactly(RESOURCE_A.attributes(), RESOURCE_B.attributes());
        }
        assertThat(proto.dataMaterialized()).isFalse();
    }
}
