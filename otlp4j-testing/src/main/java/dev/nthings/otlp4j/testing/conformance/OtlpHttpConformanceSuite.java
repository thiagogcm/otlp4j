package dev.nthings.otlp4j.testing.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.time.Duration;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// The reusable OTLP/HTTP conformance suite: one assertion per pinned wire behavior of the current
/// HTTP server, driven entirely over the wire through [HttpWireClient].
///
/// A concrete runner in otlp4j-conformance subclasses this and supplies [#boot] with a real
/// gateway; this class names no receiver, exporter, pipeline, or result type. Every case boots a
/// fresh gateway for its scenario and closes it, so admission counters and TLS contexts never leak
/// between cases.
public abstract class OtlpHttpConformanceSuite {

    /// Boots a gateway configured by `scenario` on the given transport. The concrete runner
    /// implements this by delegating to the one seam implementation; this suite never names it.
    ///
    /// @param scenario the neutral scenario to configure the gateway with
    /// @param protocol the single transport to boot
    /// @return the booted gateway, which the caller closes
    protected abstract GatewayUnderTest boot(Scenario scenario, Protocol protocol);

    @ParameterizedTest
    @EnumSource(ConformanceSignal.class)
    void acceptedReturns200(ConformanceSignal signal) {
        var scenario = Scenario.builder(signal).outcome(new ProgrammedOutcome.Accepted()).build();
        withHttp(scenario, (client, gw) -> {
            var response = client.export(signal);
            assertThat(response.httpStatus()).hasValue(200);
            assertThat(hasPartialSuccess(signal, response)).isFalse();
        });
    }

    @ParameterizedTest
    @EnumSource(ConformanceSignal.class)
    void partialReturns200WithPartialSuccess(ConformanceSignal signal) {
        var scenario = Scenario.builder(signal)
                .outcome(new ProgrammedOutcome.Partial(7, "7 items rejected"))
                .build();
        withHttp(scenario, (client, gw) -> {
            var response = client.export(signal);
            assertThat(response.httpStatus()).hasValue(200);
            assertThat(hasPartialSuccess(signal, response)).isTrue();
            assertThat(rejectedCount(signal, response)).isEqualTo(7L);
        });
    }

    @Test
    void retryableRejectionReturns503WithoutRetryAfter() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Retryable("try again"))
                .build();
        withHttp(scenario, (client, gw) -> {
            var response = client.export(ConformanceSignal.TRACES);
            assertThat(response.httpStatus()).hasValue(503);
            assertThat(response.retryAfterHeader()).isEmpty();
        });
    }

    @Test
    void throttledRejectionReturns503WithRetryAfter() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Throttled(Duration.ofSeconds(3), "slow down"))
                .build();
        withHttp(scenario, (client, gw) -> {
            var response = client.export(ConformanceSignal.TRACES);
            assertThat(response.httpStatus()).hasValue(503);
            assertThat(response.retryAfterHeader()).hasValue("3");
        });
    }

    @Test
    void permanentRejectionReturns500() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Permanent("bad request"))
                .build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).httpStatus()).hasValue(500));
    }

    @Test
    void dispatcherExceptionReturns500() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.ThrowRuntime("boom"))
                .build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).httpStatus()).hasValue(500));
    }

    @Test
    void admissionRefusalReturns429WithRetryAfter() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Accepted())
                .holdFirstRequest(true)
                .maxInFlightRequests(1)
                .throttleRetryAfter(Duration.ofSeconds(2))
                .build();
        withHttp(scenario, (client, gw) -> {
            var held = client.exportAsync(ConformanceSignal.TRACES);
            // Wait until the held request has actually taken the single permit before probing, so
            // the probe cannot win the permit itself and invert the test.
            gw.awaitHeldRequest();
            // Once the held request occupies the single permit, a second request is refused.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var refused = client.export(ConformanceSignal.TRACES);
                assertThat(refused.httpStatus()).hasValue(429);
                assertThat(refused.retryAfterHeader()).hasValue("2");
            });
            gw.releaseHeldRequest();
            assertThat(held.join().httpStatus()).hasValue(200);
            assertThat(client.export(ConformanceSignal.TRACES).httpStatus()).hasValue(200);
        });
    }

    @Test
    void oversizedPayloadReturns413() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .maxInboundMessageSizeBytes(1024)
                .build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.exportLarge(ConformanceSignal.TRACES, 8192).httpStatus()).hasValue(413));
    }

    @Test
    void gzipRequestIsInflatedAndReturns200() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.exportGzip(ConformanceSignal.TRACES).httpStatus()).hasValue(200));
    }

    @Test
    void jsonContentTypeReturns415() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).build();
        withHttp(scenario, (client, gw) -> assertThat(
                client.exportWithContentType(ConformanceSignal.TRACES, "application/json").httpStatus())
                .hasValue(415));
    }

    @Test
    void missingContentTypeIsTolerated() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).build();
        withHttp(scenario, (client, gw) -> assertThat(
                client.exportWithContentType(ConformanceSignal.TRACES, null).httpStatus())
                .hasValue(200));
    }

    @Test
    void getMethodReturns405() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.get(ConformanceSignal.TRACES).httpStatus()).hasValue(405));
    }

    @Test
    void malformedProtobufReturns400() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).build();
        var garbage = new byte[] {(byte) 0x08, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        withHttp(scenario, (client, gw) ->
                assertThat(client.exportRaw(ConformanceSignal.TRACES, garbage).httpStatus()).hasValue(400));
    }

    @Test
    void tlsServerAuthSucceeds() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).tls(TlsMode.SERVER_AUTH).build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).httpStatus()).hasValue(200));
    }

    @Test
    void tlsUntrustedClientFailsHandshake() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).tls(TlsMode.SERVER_AUTH).build();
        try (var gw = boot(scenario, Protocol.HTTP);
                var client = new HttpWireClient(
                        gw.httpBaseUri().orElseThrow(), TlsMode.SERVER_AUTH, TlsTestMaterial.clientCertPem())) {
            assertThatThrownBy(() -> client.export(ConformanceSignal.TRACES)).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void mutualTlsClientCertificateIsAcceptedButNotRequired() {
        // The current servers do not request or verify a client certificate, so presenting one
        // still succeeds. This pins the enforced-mTLS gap as an executable fact rather than a
        // capability the code does not have.
        var scenario = Scenario.builder(ConformanceSignal.TRACES).tls(TlsMode.MUTUAL).build();
        withHttp(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).httpStatus()).hasValue(200));
    }

    private void withHttp(Scenario scenario, BiConsumer<HttpWireClient, GatewayUnderTest> body) {
        try (var gw = boot(scenario, Protocol.HTTP);
                var client = new HttpWireClient(
                        gw.httpBaseUri().orElseThrow(), scenario.tls(), gw.serverTrustPem().orElse(null))) {
            body.accept(client, gw);
        }
    }

    private static boolean hasPartialSuccess(ConformanceSignal signal, WireResponse response) {
        var body = response.body().orElseThrow();
        try {
            return switch (signal) {
                case TRACES -> ExportTraceServiceResponse.parseFrom(body).hasPartialSuccess();
                case METRICS -> ExportMetricsServiceResponse.parseFrom(body).hasPartialSuccess();
                case LOGS -> ExportLogsServiceResponse.parseFrom(body).hasPartialSuccess();
                case PROFILES -> ExportProfilesServiceResponse.parseFrom(body).hasPartialSuccess();
            };
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("unparseable OTLP/HTTP response body", e);
        }
    }

    private static long rejectedCount(ConformanceSignal signal, WireResponse response) {
        var body = response.body().orElseThrow();
        try {
            return switch (signal) {
                case TRACES -> ExportTraceServiceResponse.parseFrom(body).getPartialSuccess().getRejectedSpans();
                case METRICS ->
                        ExportMetricsServiceResponse.parseFrom(body).getPartialSuccess().getRejectedDataPoints();
                case LOGS -> ExportLogsServiceResponse.parseFrom(body).getPartialSuccess().getRejectedLogRecords();
                case PROFILES ->
                        ExportProfilesServiceResponse.parseFrom(body).getPartialSuccess().getRejectedProfiles();
            };
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("unparseable OTLP/HTTP response body", e);
        }
    }
}
