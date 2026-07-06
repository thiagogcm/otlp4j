package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.testing.conformance.GatewayUnderTest;
import dev.nthings.otlp4j.testing.conformance.ProgrammedOutcome;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.conformance.Scenario;
import dev.nthings.otlp4j.testing.conformance.TlsMode;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import dev.nthings.otlp4j.transport.common.AbstractReceiverBuilder;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiver;
import dev.nthings.otlp4j.transport.http.OtlpHttpReceiver;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/// The single bootstrap seam implementation: the only class in the two new conformance modules that
/// names a transport or model type.
///
/// It boots the one transport a concrete suite requested on an ephemeral loopback port, configures
/// it from the neutral [Scenario], and attaches the same programmed per-signal consumer to all four
/// signals. That consumer translates the scenario's [ProgrammedOutcome] into the current runtime's
/// [ConsumeResult], so the reusable suites in otlp4j-testing observe only the wire status the
/// gateway emits. When Phase 1 replaces the fluent runtime with the service graph, only this class
/// changes: it will target the neutral delivery result instead, and every conformance test compiles
/// and passes unchanged.
public final class Otlp4jGatewayUnderTest implements GatewayUnderTest {

    private final Receiver receiver;
    private final Protocol protocol;
    private final boolean tlsEnabled;
    private final ProgrammedOutcome outcome;
    private final AtomicReference<CompletableFuture<ConsumeResult>> heldRequest;
    private final CountDownLatch heldRequestArmed;

    private Otlp4jGatewayUnderTest(
            Receiver receiver,
            Protocol protocol,
            boolean tlsEnabled,
            ProgrammedOutcome outcome,
            AtomicReference<CompletableFuture<ConsumeResult>> heldRequest,
            CountDownLatch heldRequestArmed) {
        this.receiver = receiver;
        this.protocol = protocol;
        this.tlsEnabled = tlsEnabled;
        this.outcome = outcome;
        this.heldRequest = heldRequest;
        this.heldRequestArmed = heldRequestArmed;
    }

    /// Boots a gateway on the single requested transport, bound to an ephemeral loopback port and
    /// configured by the scenario. This is the concrete factory the two abstract suites reach
    /// through their protected `boot` method; it is a static method on the implementation, not on
    /// the seam, so the suites never name this type.
    ///
    /// @param scenario the neutral scenario to configure the gateway with
    /// @param protocol the single transport to boot
    /// @return the booted gateway, which the caller closes
    public static GatewayUnderTest boot(Scenario scenario, Protocol protocol) {
        var heldRequest = new AtomicReference<CompletableFuture<ConsumeResult>>();
        var heldRequestArmed = new CountDownLatch(1);
        var receiver = switch (protocol) {
            case HTTP -> buildReceiver(OtlpHttpReceiver.builder(), scenario, heldRequest, heldRequestArmed);
            case GRPC -> buildReceiver(OtlpGrpcReceiver.builder(), scenario, heldRequest, heldRequestArmed);
        };
        return new Otlp4jGatewayUnderTest(
                receiver,
                protocol,
                scenario.tls() != TlsMode.NONE,
                scenario.outcome(),
                heldRequest,
                heldRequestArmed);
    }

    private static <B extends AbstractReceiverBuilder<B>> Receiver buildReceiver(
            B builder,
            Scenario scenario,
            AtomicReference<CompletableFuture<ConsumeResult>> heldRequest,
            CountDownLatch heldRequestArmed) {
        builder.setEndpoint("127.0.0.1", 0)
                .setMaxInboundMessageSizeBytes(scenario.maxInboundMessageSizeBytes())
                .setMaxInFlightRequests(scenario.maxInFlightRequests())
                .setMaxInFlightBytes(scenario.maxInFlightBytes())
                .onTraces(programmedSink(scenario, heldRequest, heldRequestArmed))
                .onMetrics(programmedSink(scenario, heldRequest, heldRequestArmed))
                .onLogs(programmedSink(scenario, heldRequest, heldRequestArmed))
                .onProfiles(programmedSink(scenario, heldRequest, heldRequestArmed));
        if (scenario.throttleRetryAfter() != null) {
            builder.setThrottleRetryAfter(scenario.throttleRetryAfter());
        }
        if (scenario.tls() != TlsMode.NONE) {
            builder.setTls(Tls.custom(
                    TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null));
        }
        return builder.build().start();
    }

    /// Builds the per-signal consumer the receiver dispatches to. Every signal shares one instance
    /// of this logic; only the scenario's own signal is exercised by any given case.
    private static <T> Sink<T> programmedSink(
            Scenario scenario,
            AtomicReference<CompletableFuture<ConsumeResult>> heldRequest,
            CountDownLatch heldRequestArmed) {
        return batch -> dispatch(scenario, heldRequest, heldRequestArmed);
    }

    private static CompletionStage<ConsumeResult> dispatch(
            Scenario scenario,
            AtomicReference<CompletableFuture<ConsumeResult>> heldRequest,
            CountDownLatch heldRequestArmed) {
        if (scenario.holdFirstRequest()) {
            var future = new CompletableFuture<ConsumeResult>();
            if (heldRequest.compareAndSet(null, future)) {
                // Hold the first admitted request open so it keeps its admission permit while the
                // test sends a second request that must be refused. The test releases it later.
                // Signal that the permit is now held so a probe cannot race ahead of it and win
                // the single permit itself.
                heldRequestArmed.countDown();
                return future;
            }
        }
        return CompletableFuture.completedFuture(toResult(scenario.outcome()));
    }

    private static ConsumeResult toResult(ProgrammedOutcome outcome) {
        return switch (outcome) {
            case ProgrammedOutcome.Accepted ignored -> ConsumeResult.accepted();
            case ProgrammedOutcome.Partial partial ->
                    ConsumeResult.partial(partial.rejectedItems(), partial.message());
            case ProgrammedOutcome.Retryable retryable -> ConsumeResult.retryable(retryable.message());
            case ProgrammedOutcome.Throttled throttled ->
                    ConsumeResult.throttled(throttled.retryAfter(), throttled.message());
            case ProgrammedOutcome.Permanent permanent -> ConsumeResult.permanent(permanent.message());
            case ProgrammedOutcome.ThrowRuntime thrown -> throw new RuntimeException(thrown.message());
        };
    }

    @Override
    public Optional<URI> httpBaseUri() {
        if (protocol != Protocol.HTTP) {
            return Optional.empty();
        }
        var scheme = tlsEnabled ? "https" : "http";
        return Optional.of(URI.create(scheme + "://127.0.0.1:" + receiver.port()));
    }

    @Override
    public Optional<String> grpcTarget() {
        return protocol == Protocol.GRPC ? Optional.of("127.0.0.1:" + receiver.port()) : Optional.empty();
    }

    @Override
    public Optional<byte[]> serverTrustPem() {
        return tlsEnabled ? Optional.of(TlsTestMaterial.caPem()) : Optional.empty();
    }

    @Override
    public void awaitHeldRequest() {
        try {
            if (!heldRequestArmed.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the held request was never admitted within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting the held request", e);
        }
    }

    @Override
    public void releaseHeldRequest() {
        var future = heldRequest.get();
        if (future != null) {
            future.complete(toResult(outcome));
        }
    }

    @Override
    public void close() {
        receiver.shutdownNow().toCompletableFuture().join();
    }
}
