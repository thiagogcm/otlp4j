package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins the agreement between the proto-walking item counter and the domain item counter: for the
/// same telemetry a [ProtoRequestPayload] and a [DomainRequestPayload] report the same
/// [dev.nthings.otlp4j.core.signal.TelemetryPayload#itemCount].
///
/// The logs and profiles fixtures deliberately carry more than one scope per resource, so a proto
/// counter that flattened the resource then scope then item hierarchy to two levels would miscount
/// and fail here rather than pass by accident on a single-scope fixture.
@DisplayName("Payload item count parity")
class PayloadItemCountParityTest {

    @DisplayName("traces item count agrees across proto-backed and domain-backed payloads")
    @Test
    void traces() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER),
                Fixtures.span("b", Span.Kind.CLIENT),
                Fixtures.span("c", Span.Kind.INTERNAL));
        assertParity(SignalCodec.TRACES, batch, 3L);
    }

    @DisplayName("metrics data point count agrees across proto-backed and domain-backed payloads")
    @Test
    void metrics() {
        var batch = Fixtures.metricsData(Fixtures.metric("m1"), Fixtures.metric("m2"));
        assertParity(SignalCodec.METRICS, batch, 2L);
    }

    @DisplayName("logs record count agrees, with more than one scope per resource")
    @Test
    void logs() {
        var record = Fixtures.logRecord("body", LogRecord.Severity.INFO);
        var scope = Fixtures.testScope();
        var resource = Fixtures.checkoutResource();
        var scopeA = new LogsData.ScopeLogs(scope, "", List.of(record, record));
        var scopeB = new LogsData.ScopeLogs(scope, "", List.of(record));
        var batch = new LogsData(List.of(new LogsData.ResourceLogs(resource, "", List.of(scopeA, scopeB))));
        assertParity(SignalCodec.LOGS, batch, 3L);
    }

    @DisplayName("profiles count agrees, with more than one scope per resource")
    @Test
    void profiles() {
        var profile = Fixtures.profile("0102030405060708");
        var scope = Fixtures.testScope();
        var resource = Fixtures.checkoutResource();
        var scopeA = new ProfilesData.ScopeProfiles(scope, "", List.of(profile, profile));
        var scopeB = new ProfilesData.ScopeProfiles(scope, "", List.of(profile));
        var batch = new ProfilesData(
                List.of(new ProfilesData.ResourceProfiles(resource, "", List.of(scopeA, scopeB))), new byte[0]);
        assertParity(SignalCodec.PROFILES, batch, 3L);
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertParity(
            SignalCodec<T, REQ, RESP> codec, T batch, long expected) {
        var request = codec.toProto(batch);
        var proto = new ProtoRequestPayload<>(codec, request);
        var domain = new DomainRequestPayload<>(codec, batch);

        assertThat(proto.itemCount()).as("proto-backed item count").isEqualTo(expected);
        assertThat(domain.itemCount()).as("domain-backed item count").isEqualTo(expected);
        assertThat(proto.itemCount()).as("proto and domain counts agree").isEqualTo(domain.itemCount());
    }
}
