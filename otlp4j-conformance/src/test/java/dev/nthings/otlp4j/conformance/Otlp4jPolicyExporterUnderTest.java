package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.telemetry.LoggingGatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.policy.ExportObservation;
import dev.nthings.otlp4j.testing.policy.ExportProgram;
import dev.nthings.otlp4j.testing.policy.PolicyExporterUnderTest;
import dev.nthings.otlp4j.testing.policy.ProgrammedOtlpServer;
import dev.nthings.otlp4j.transport.common.ClientExporter;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcExporter;
import dev.nthings.otlp4j.transport.http.OtlpHttpExporter;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

/// The single policy-suite seam implementation: the only class in the conformance module that names a
/// real exporter type for the policy semantics suite. It boots a [ProgrammedOtlpServer] on an ephemeral
/// loopback port, configures a real exporter with the program's neutral policies pointed at that port
/// through the same public entry points a user calls, drives exactly one export of a fixed trace batch,
/// waits for the caller stage to settle, tears the exporter down under a bounded grace so any straggler
/// retry is reclaimed, snapshots the server's recorded arrivals, and returns the neutral observation.
///
/// One instance drives one protocol. Its capability declarations describe what the legacy transport
/// does today: legacy gRPC ignores gRPC RetryInfo, HTTP folds the RetryInfo-equivalent `Retry-After`,
/// and neither legacy transport cancels an already-scheduled retry at the total deadline.
final class Otlp4jPolicyExporterUnderTest implements PolicyExporterUnderTest {

    /// Bounded grace for tearing the exporter down between the caller stage settling and the arrival
    /// snapshot, so a straggler scheduled after the total deadline is reclaimed rather than left to
    /// arrive during the snapshot.
    private static final Duration TEARDOWN_GRACE = Duration.ofSeconds(1);

    /// The exporter retry instrument the real wire client feeds from its Resilience4j retry listener.
    /// Reading it back through the seam's own telemetry registry is what pins the client's
    /// onRetry to recordRetry wiring end to end, not merely the direct ExporterMetrics unit test.
    private static final String RETRIES_COUNTER = "otlp4j.exporter.export.retries";

    private final Protocol protocol;

    Otlp4jPolicyExporterUnderTest(Protocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public ExportObservation runOnce(ExportProgram program) {
        var server = ProgrammedOtlpServer.start(protocol, program.script());
        try {
            // A real recording registry, so the retry attempts the wire client emits through its
            // Resilience4j listener land in a counter the observation can read back.
            var telemetry = new LoggingGatewayMetrics();
            var exporter = buildExporter(server.port(), program, telemetry);
            DeliveryResult result = null;
            Throwable failure = null;
            long start = System.nanoTime();
            try {
                result = exporter.export(SignalDescriptor.TRACES, batch()).toCompletableFuture().join();
            } catch (CompletionException | CancellationException e) {
                failure = e;
            }
            long elapsedNanos = System.nanoTime() - start;
            // Tear the exporter down under a bounded grace before snapshotting, so an in-flight straggler
            // is interrupted by the executor drain instead of arriving during the snapshot.
            try {
                exporter.shutdown(TEARDOWN_GRACE).toCompletableFuture().join();
            } catch (RuntimeException ignored) {
                // A teardown timeout already interrupts the transport; the snapshot below is still valid.
            }
            return ExportObservation.of(
                    server.attempts(),
                    server.interAttemptGaps(),
                    Duration.ofNanos(elapsedNanos),
                    result,
                    failure,
                    program.retry().maxAttempts(),
                    (int) telemetry.counterValue(RETRIES_COUNTER));
        } finally {
            server.close();
        }
    }

    /// Builds a real exporter pointed at the programmed server, configured with the program's policies
    /// through the public config surface: the per-attempt timeout maps to the client timeout and the
    /// total timeout to the total budget, so the interim translation completed in Lot 1 is exercised.
    /// The exporter reports to `telemetry`, so the real wire client's per-attempt retry counting is
    /// observable through the exporter's `otlp4j.exporter.export.retries` instrument.
    private ClientExporter buildExporter(int port, ExportProgram program, LoggingGatewayMetrics telemetry) {
        var config = ClientConfig.builder()
                .setHost("127.0.0.1")
                .setPort(port)
                .setTimeout(program.timeout().perAttempt())
                .setTotalTimeout(program.timeout().total())
                .setRetryPolicy(program.retry())
                .build();
        return switch (protocol) {
            case GRPC -> OtlpGrpcExporter.builder().setConfig(config).telemetry(telemetry).build();
            case HTTP -> OtlpHttpExporter.builder().setConfig(config).telemetry(telemetry).build();
        };
    }

    private static TracesData batch() {
        return Fixtures.traceData(Fixtures.span("policy-probe", Span.Kind.SERVER));
    }

    @Override
    public Protocol protocol() {
        return protocol;
    }

    @Override
    public boolean honorsGrpcRetryInfo() {
        // Legacy gRPC ignores grpc-status-details-bin RetryInfo; HTTP folds the equivalent Retry-After.
        return protocol == Protocol.HTTP;
    }

    @Override
    public boolean cancelsRetriesAtTotalDeadline() {
        // Neither legacy transport interrupts an already-scheduled retry at the total deadline.
        return false;
    }

    @Override
    public void close() {
        // No shared fixture: each runOnce owns and closes its server and exporter.
    }
}
