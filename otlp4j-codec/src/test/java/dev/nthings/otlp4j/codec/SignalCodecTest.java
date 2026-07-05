package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Aggregation coverage for [SignalCodec]: each codec binds the right [Signal]; `toProto` →
/// `parseRequest` → `toDomain` is an identity on a fixture; `response`/`result` round-trip accepted
/// and partial [ConsumeResult]s; and `values()` lists all four in wire order.
@DisplayName("SignalCodec")
class SignalCodecTest {

    @DisplayName("values() lists the four codecs bound to their signals in wire order")
    @Test
    void valuesAndSignalBindings() {
        assertThat(SignalCodec.values())
                .containsExactly(
                        SignalCodec.TRACES, SignalCodec.METRICS, SignalCodec.LOGS, SignalCodec.PROFILES);
        assertThat(SignalCodec.TRACES.signal()).isEqualTo(Signal.TRACES);
        assertThat(SignalCodec.METRICS.signal()).isEqualTo(Signal.METRICS);
        assertThat(SignalCodec.LOGS.signal()).isEqualTo(Signal.LOGS);
        assertThat(SignalCodec.PROFILES.signal()).isEqualTo(Signal.PROFILES);
    }

    @DisplayName("traces round-trip through toProto/parseRequest/toDomain and response/result")
    @Test
    void tracesCodec() {
        TracesData fixture = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        assertRequestRoundTrip(SignalCodec.TRACES, fixture);
        assertResponseRoundTrip(SignalCodec.TRACES);
    }

    @DisplayName("metrics round-trip through toProto/parseRequest/toDomain and response/result")
    @Test
    void metricsCodec() {
        MetricsData fixture = Fixtures.metricsData(Fixtures.metric("a"));
        assertRequestRoundTrip(SignalCodec.METRICS, fixture);
        assertResponseRoundTrip(SignalCodec.METRICS);
    }

    @DisplayName("logs round-trip through toProto/parseRequest/toDomain and response/result")
    @Test
    void logsCodec() {
        LogsData fixture = Fixtures.logsData(Fixtures.logRecord("a", LogRecord.Severity.INFO));
        assertRequestRoundTrip(SignalCodec.LOGS, fixture);
        assertResponseRoundTrip(SignalCodec.LOGS);
    }

    @DisplayName("profiles round-trip through toProto/parseRequest/toDomain and response/result")
    @Test
    void profilesCodec() {
        ProfilesData fixture = Fixtures.profilesData(Fixtures.profile("01"));
        assertRequestRoundTrip(SignalCodec.PROFILES, fixture);
        assertResponseRoundTrip(SignalCodec.PROFILES);
    }

    /// Exercises the full `toProto` → wire-encode → `parseRequest` → `toDomain` chain and asserts a
    /// stable fixed point: re-encoding the decoded batch reproduces the original wire bytes. This is
    /// signal-uniform — traces/metrics/logs also round-trip at the domain level, but profiles
    /// forward opaquely (a scalar-only profile materializes its `rawProfile` on the first `toProto`),
    /// so the byte fixed point is the invariant all four share. `parseRequest` reconstructs the same
    /// proto it was handed.
    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertRequestRoundTrip(
            SignalCodec<T, REQ, RESP> codec, T fixture) {
        try {
            REQ request = codec.toProto(fixture);
            byte[] bytes = request.toByteArray();
            REQ parsed = codec.parseRequest(bytes);
            assertThat(parsed.toByteArray())
                    .as("parseRequest(bytes) must reconstruct the same proto")
                    .isEqualTo(bytes);
            T decoded = codec.toDomain(parsed);
            assertThat(codec.toProto(decoded).toByteArray())
                    .as("toProto(toDomain(parseRequest(toProto(x).bytes))) must reproduce the wire bytes")
                    .isEqualTo(bytes);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /// `response` builds a proto message a peer would send; `result` (directly and after a wire
    /// round-trip via `parseResponse`) must recover the original [ConsumeResult] shape.
    private static <T, REQ extends MessageLite, RESP extends MessageLite> void assertResponseRoundTrip(
            SignalCodec<T, REQ, RESP> codec) {
        try {
            RESP accepted = codec.response(ConsumeResult.accepted());
            assertThat(codec.result(accepted)).isEqualTo(ConsumeResult.accepted());
            assertThat(codec.result(codec.parseResponse(accepted.toByteArray())))
                    .isEqualTo(ConsumeResult.accepted());

            RESP partial = codec.response(ConsumeResult.partial(2, "rejected"));
            assertThat(codec.result(partial)).isEqualTo(ConsumeResult.partial(2, "rejected"));
            assertThat(codec.result(codec.parseResponse(partial.toByteArray())))
                    .isEqualTo(ConsumeResult.partial(2, "rejected"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @DisplayName("an empty batch round-trips through each codec")
    @Test
    void emptyBatchesRoundTrip() {
        assertRequestRoundTrip(SignalCodec.TRACES, new TracesData(List.of()));
        assertRequestRoundTrip(SignalCodec.METRICS, new MetricsData(List.of()));
        assertRequestRoundTrip(SignalCodec.LOGS, new LogsData(List.of()));
        assertRequestRoundTrip(SignalCodec.PROFILES, new ProfilesData(List.of(), new byte[0]));
    }
}
