package dev.nthings.otlp4j.testing.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.time.Duration;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// The reusable OTLP/gRPC conformance suite: one assertion per pinned wire behavior of the current
/// gRPC server, driven entirely through the generated stubs in [GrpcWireClient].
///
/// A concrete runner in otlp4j-conformance subclasses this and supplies [#boot] with a real
/// gateway; this class names no receiver, exporter, pipeline, or result type. Both an admission
/// refusal and a plain retryable rejection map to `UNAVAILABLE`, so the admission case is
/// distinguished by the status description carrying "receiver at capacity".
public abstract class OtlpGrpcConformanceSuite {

    /// Boots a gateway configured by `scenario` on the given transport. The concrete runner
    /// implements this by delegating to the one seam implementation; this suite never names it.
    ///
    /// @param scenario the neutral scenario to configure the gateway with
    /// @param protocol the single transport to boot
    /// @return the booted gateway, which the caller closes
    protected abstract GatewayUnderTest boot(Scenario scenario, Protocol protocol);

    @ParameterizedTest
    @EnumSource(ConformanceSignal.class)
    void acceptedReturnsOk(ConformanceSignal signal) {
        var scenario = Scenario.builder(signal).outcome(new ProgrammedOutcome.Accepted()).build();
        withGrpc(scenario, (client, gw) -> {
            var response = client.export(signal);
            assertThat(response.grpcCode()).hasValue(Status.Code.OK);
            assertThat(hasPartialSuccess(signal, response)).isFalse();
        });
    }

    @ParameterizedTest
    @EnumSource(ConformanceSignal.class)
    void partialReturnsOkWithPartialSuccess(ConformanceSignal signal) {
        var scenario = Scenario.builder(signal)
                .outcome(new ProgrammedOutcome.Partial(1, "one item rejected"))
                .build();
        withGrpc(scenario, (client, gw) -> {
            var response = client.export(signal);
            assertThat(response.grpcCode()).hasValue(Status.Code.OK);
            assertThat(rejectedCount(signal, response)).isEqualTo(1L);
        });
    }

    @Test
    void retryableRejectionReturnsUnavailableWithoutTrailer() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Retryable("try again"))
                .build();
        withGrpc(scenario, (client, gw) -> {
            var response = client.export(ConformanceSignal.TRACES);
            assertThat(response.grpcCode()).hasValue(Status.Code.UNAVAILABLE);
            assertThat(response.pushbackMillis()).isEmpty();
        });
    }

    @Test
    void throttledRejectionReturnsUnavailableWithPushbackTrailer() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Throttled(Duration.ofSeconds(3), "slow down"))
                .build();
        withGrpc(scenario, (client, gw) -> {
            var response = client.export(ConformanceSignal.TRACES);
            assertThat(response.grpcCode()).hasValue(Status.Code.UNAVAILABLE);
            assertThat(response.pushbackMillis()).hasValue(3000L);
        });
    }

    @Test
    void permanentRejectionReturnsInternal() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Permanent("bad request"))
                .build();
        withGrpc(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.INTERNAL));
    }

    @Test
    void dispatcherExceptionReturnsInternal() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.ThrowRuntime("boom"))
                .build();
        withGrpc(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.INTERNAL));
    }

    @Test
    void admissionRefusalReturnsUnavailableWithPushbackThenRecovers() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .outcome(new ProgrammedOutcome.Accepted())
                .holdFirstRequest(true)
                .maxInFlightRequests(1)
                .throttleRetryAfter(Duration.ofSeconds(2))
                .build();
        withGrpc(scenario, (client, gw) -> {
            var held = client.exportAsync(ConformanceSignal.TRACES);
            // Wait until the held request has actually taken the single permit before probing, so
            // the probe cannot win the permit itself and invert the test.
            gw.awaitHeldRequest();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var refused = client.export(ConformanceSignal.TRACES);
                assertThat(refused.grpcCode()).hasValue(Status.Code.UNAVAILABLE);
                assertThat(refused.grpcDescription()).hasValueSatisfying(
                        description -> assertThat(description).contains("receiver at capacity"));
                assertThat(refused.pushbackMillis()).hasValue(2000L);
            });
            gw.releaseHeldRequest();
            assertThat(held.join().grpcCode()).hasValue(Status.Code.OK);
            assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.OK);
        });
    }

    @Test
    void oversizedPayloadReturnsResourceExhausted() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES)
                .maxInboundMessageSizeBytes(1024)
                .build();
        withGrpc(scenario, (client, gw) -> assertThat(
                client.exportLarge(ConformanceSignal.TRACES, 8192).grpcCode())
                .hasValue(Status.Code.RESOURCE_EXHAUSTED));
    }

    @Test
    void gzipRequestIsInflatedAndReturnsOk() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).build();
        withGrpc(scenario, (client, gw) ->
                assertThat(client.exportGzip(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.OK));
    }

    @Test
    void tlsServerAuthSucceeds() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).tls(TlsMode.SERVER_AUTH).build();
        withGrpc(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.OK));
    }

    @Test
    void tlsUntrustedClientFailsHandshake() {
        var scenario = Scenario.builder(ConformanceSignal.TRACES).tls(TlsMode.SERVER_AUTH).build();
        try (var gw = boot(scenario, Protocol.GRPC);
                var client = new GrpcWireClient(
                        gw.grpcTarget().orElseThrow(), TlsMode.SERVER_AUTH, TlsTestMaterial.clientCertPem())) {
            assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.UNAVAILABLE);
        }
    }

    @Test
    void mutualTlsClientCertificateIsAcceptedButNotRequired() {
        // The current servers do not request or verify a client certificate, so presenting one
        // still succeeds. This pins the enforced-mTLS gap as an executable fact.
        var scenario = Scenario.builder(ConformanceSignal.TRACES).tls(TlsMode.MUTUAL).build();
        withGrpc(scenario, (client, gw) ->
                assertThat(client.export(ConformanceSignal.TRACES).grpcCode()).hasValue(Status.Code.OK));
    }

    private void withGrpc(Scenario scenario, BiConsumer<GrpcWireClient, GatewayUnderTest> body) {
        try (var gw = boot(scenario, Protocol.GRPC);
                var client = new GrpcWireClient(
                        gw.grpcTarget().orElseThrow(), scenario.tls(), gw.serverTrustPem().orElse(null))) {
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
            throw new AssertionError("unparseable OTLP/gRPC response body", e);
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
            throw new AssertionError("unparseable OTLP/gRPC response body", e);
        }
    }
}
