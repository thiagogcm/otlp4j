package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.transport.ClientAuth;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import dev.nthings.otlp4j.testing.conformance.GatewayUnderTest;
import dev.nthings.otlp4j.testing.conformance.ProgrammedOutcome;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.conformance.Scenario;
import dev.nthings.otlp4j.testing.conformance.TlsMode;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/// The single bootstrap seam implementation: the only class in the two new conformance modules that
/// names a transport, service, or core runtime type.
///
/// It boots the one transport a concrete suite requested on an ephemeral loopback port through the
/// service graph runtime. The graph is a minimal gateway: one OTLP receiver, bound from the neutral
/// [Scenario], routed to a programmed terminal [ExporterFactory] on every signal, one pipeline per
/// signal. The programmed exporter is the graph-runtime equivalent of the old programmed sink: it
/// translates the scenario's [ProgrammedOutcome] into a [DeliveryResult], which the receiver adapter
/// bridges back to the same wire status the gateway emits today. The reusable suites in otlp4j-testing
/// observe only that wire status, so they compile and pass unchanged on the graph runtime.
///
/// The bound ephemeral port is read back through the runtime's component lookup, downcasting the live
/// receiver to [NetworkBound], which is the graph-runtime equivalent of the retired `Receiver.port()`.
public final class Otlp4jGatewayUnderTest implements GatewayUnderTest {

    private static final ComponentId RECEIVER_ID = ComponentId.parse("in");
    private static final ComponentId EXPORTER_ID = ComponentId.parse("out");

    private final GatewayRuntime runtime;
    private final Protocol protocol;
    private final boolean tlsEnabled;
    private final ProgrammedOutcome outcome;
    private final AtomicReference<Held> heldRequest;
    private final CountDownLatch heldRequestArmed;

    private Otlp4jGatewayUnderTest(
            GatewayRuntime runtime,
            Protocol protocol,
            boolean tlsEnabled,
            ProgrammedOutcome outcome,
            AtomicReference<Held> heldRequest,
            CountDownLatch heldRequestArmed) {
        this.runtime = runtime;
        this.protocol = protocol;
        this.tlsEnabled = tlsEnabled;
        this.outcome = outcome;
        this.heldRequest = heldRequest;
        this.heldRequestArmed = heldRequestArmed;
    }

    /// Boots a gateway on the single requested transport, bound to an ephemeral loopback port and
    /// configured by the scenario. This is the concrete factory the two abstract suites reach through
    /// their protected `boot` method; it is a static method on the implementation, not on the seam, so
    /// the suites never name this type. It boots the consolidated Armeria transport, the only
    /// production transport; everything else in the seam (the programmed terminal exporter, the graph
    /// assembly, the port readback, and the TLS material) is transport-agnostic.
    ///
    /// @param scenario the neutral scenario to configure the gateway with
    /// @param protocol the single transport to boot
    /// @return the booted gateway, which the caller closes
    public static GatewayUnderTest boot(Scenario scenario, Protocol protocol) {
        var heldRequest = new AtomicReference<Held>();
        var heldRequestArmed = new CountDownLatch(1);
        ReceiverFactory<OtlpReceiverConfig> receiverFactory = switch (protocol) {
            case HTTP -> OtlpReceiverFactory.http();
            case GRPC -> OtlpReceiverFactory.grpc();
        };
        // The programmed terminal exporter, built inline through ExporterFactory.of. On consume it
        // either holds the first admitted request open as an incomplete delivery stage, keeping the
        // receiver's admission permit so a second request is refused, or completes immediately with the
        // scenario's programmed outcome. A ThrowRuntime outcome throws synchronously, which the graph
        // node boundary normalizes to a permanent rejection, the same wire status the old thrown-from-
        // sink path produced.
        var exporterFactory = ExporterFactory.of("programmed", (ctx, data, provenance) -> {
            if (scenario.holdFirstRequest()) {
                var future = new CompletableFuture<DeliveryResult>();
                if (heldRequest.compareAndSet(null, new Held(future, provenance))) {
                    // Hold the first admitted request open so it keeps its admission permit while the
                    // test sends a second request that must be refused. The test releases it later, and
                    // arming the latch signals that the permit is now held so a probe cannot race ahead.
                    heldRequestArmed.countDown();
                    return future;
                }
            }
            return CompletableFuture.completedFuture(toResult(scenario.outcome(), provenance));
        });

        var gw = Gateway.builder()
                // Shut down immediately on close, matching the old seam's shutdownNow(): a sub-second
                // deadline truncates to a zero-second HttpServer.stop(0), so teardown never blocks the
                // full default deadline waiting for a lingering keep-alive connection to drain.
                .service(new ServiceOptions(Duration.ofSeconds(30), Duration.ofMillis(200)));
        gw.receiver(RECEIVER_ID, receiverFactory, receiverConfig(scenario));
        gw.exporter(EXPORTER_ID, exporterFactory);
        // Route every signal through the programmed exporter, so any signal a case drives is served,
        // exactly as the old seam attached the same programmed sink to all four signals.
        for (var signal : SignalDescriptor.values()) {
            gw.pipeline(
                    PipelineId.of(signal),
                    new PipelineConfig(List.of(RECEIVER_ID), List.of(), List.of(EXPORTER_ID)));
        }
        var runtime = gw.build();
        runtime.start();
        return new Otlp4jGatewayUnderTest(
                runtime,
                protocol,
                scenario.tls() != TlsMode.NONE,
                scenario.outcome(),
                heldRequest,
                heldRequestArmed);
    }

    /// Translates the neutral scenario into the neutral OTLP receiver config: an ephemeral loopback
    /// bind, the scenario's hardening limits and optional throttle hint, and the scenario's TLS mode
    /// materialized from the test certificate when enabled.
    private static OtlpReceiverConfig receiverConfig(Scenario scenario) {
        var tls = scenario.tls() != TlsMode.NONE
                ? Tls.custom(TlsTestMaterial.serverCertPem(), TlsTestMaterial.serverKeyPem(), null)
                : Tls.disabled();
        return new OtlpReceiverConfig(
                "127.0.0.1",
                0,
                tls,
                ClientAuth.NONE,
                scenario.maxInboundMessageSizeBytes(),
                scenario.maxInFlightRequests(),
                scenario.maxInFlightBytes(),
                scenario.throttleRetryAfter());
    }

    /// The bound ephemeral port of the started receiver, read through the runtime's component lookup
    /// and the receiver's [NetworkBound] capability, the graph-runtime equivalent of the old fluent
    /// `receiver.port()`.
    private int boundPort() {
        return runtime.boundPort(RECEIVER_ID)
                .orElseThrow(() -> new IllegalStateException("the receiver did not expose a bound port"));
    }

    private static DeliveryResult toResult(ProgrammedOutcome outcome, Provenance provenance) {
        return switch (outcome) {
            case ProgrammedOutcome.Accepted ignored -> DeliveryResult.accepted(provenance);
            case ProgrammedOutcome.Partial partial ->
                    DeliveryResult.partial(partial.rejectedItems(), partial.message(), provenance);
            case ProgrammedOutcome.Retryable retryable -> DeliveryResult.retryable(retryable.message(), provenance);
            case ProgrammedOutcome.Throttled throttled ->
                    DeliveryResult.throttled(throttled.retryAfter(), throttled.message(), provenance);
            case ProgrammedOutcome.Permanent permanent -> DeliveryResult.permanent(permanent.message(), provenance);
            case ProgrammedOutcome.ThrowRuntime thrown -> throw new RuntimeException(thrown.message());
        };
    }

    @Override
    public Optional<URI> httpBaseUri() {
        if (protocol != Protocol.HTTP) {
            return Optional.empty();
        }
        var scheme = tlsEnabled ? "https" : "http";
        return Optional.of(URI.create(scheme + "://127.0.0.1:" + boundPort()));
    }

    @Override
    public Optional<String> grpcTarget() {
        return protocol == Protocol.GRPC ? Optional.of("127.0.0.1:" + boundPort()) : Optional.empty();
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
        var held = heldRequest.get();
        if (held != null) {
            held.future().complete(toResult(outcome, held.provenance()));
        }
    }

    @Override
    public void close() {
        runtime.close();
    }

    /// The held first request: its still-incomplete delivery stage and the provenance of the exporter
    /// facet that armed it, so [#releaseHeldRequest()] completes it with a result stamped for the
    /// right signal.
    private record Held(CompletableFuture<DeliveryResult> future, Provenance provenance) {}
}
