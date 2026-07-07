package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
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
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiverFactory;
import dev.nthings.otlp4j.transport.http.OtlpHttpReceiverFactory;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
    /// configured by the scenario. This is the concrete factory the two abstract suites reach
    /// through their protected `boot` method; it is a static method on the implementation, not on
    /// the seam, so the suites never name this type.
    ///
    /// @param scenario the neutral scenario to configure the gateway with
    /// @param protocol the single transport to boot
    /// @return the booted gateway, which the caller closes
    public static GatewayUnderTest boot(Scenario scenario, Protocol protocol) {
        var heldRequest = new AtomicReference<Held>();
        var heldRequestArmed = new CountDownLatch(1);
        ReceiverFactory<OtlpReceiverConfig> receiverFactory = switch (protocol) {
            case HTTP -> new OtlpHttpReceiverFactory();
            case GRPC -> new OtlpGrpcReceiverFactory();
        };
        var exporterFactory = new ProgrammedExporterFactory(scenario, heldRequest, heldRequestArmed);

        var assembler = Gateway.builder()
                // Shut down immediately on close, matching the old seam's shutdownNow(): a sub-second
                // deadline truncates to a zero-second HttpServer.stop(0), so teardown never blocks the
                // full default deadline waiting for a lingering keep-alive connection to drain.
                .service(new ServiceOptions(Duration.ofSeconds(30), Duration.ofMillis(200), List.of()))
                .receiver(RECEIVER_ID, receiverFactory, receiverConfig(scenario))
                .exporter(EXPORTER_ID, exporterFactory, ProgrammedExporterConfig.INSTANCE);
        // Route every signal through the programmed exporter, so any signal a case drives is served,
        // exactly as the old seam attached the same programmed sink to all four signals.
        for (var signal : SignalDescriptor.values()) {
            assembler.pipeline(
                    PipelineId.of(signal),
                    new PipelineConfig(List.of(RECEIVER_ID), List.of(), List.of(EXPORTER_ID)));
        }
        var runtime = assembler.build();
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
                scenario.maxInboundMessageSizeBytes(),
                scenario.maxInFlightRequests(),
                scenario.maxInFlightBytes(),
                scenario.throttleRetryAfter(),
                Duration.ofSeconds(20));
    }

    /// The bound ephemeral port of the started receiver, read through the runtime's component lookup
    /// and the receiver's [NetworkBound] capability, the graph-runtime equivalent of the old fluent
    /// `receiver.port()`.
    private int boundPort() {
        return runtime.component(RECEIVER_ID)
                .filter(NetworkBound.class::isInstance)
                .map(NetworkBound.class::cast)
                .map(NetworkBound::port)
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

    /// The (empty) config for the programmed exporter. The programmed behaviour is carried by the
    /// factory instance, not by configuration, so this record has no fields; it exists only to satisfy
    /// the typed [ComponentConfig] contract every factory declares.
    private record ProgrammedExporterConfig() implements ComponentConfig {
        static final ProgrammedExporterConfig INSTANCE = new ProgrammedExporterConfig();
    }

    /// The programmed terminal exporter factory. It builds one exporter facet per signal, all sharing
    /// the scenario and the held-request state, so any signal a case drives yields the programmed
    /// outcome and the hold-first-request scenario is arm-once across signals.
    private static final class ProgrammedExporterFactory implements ExporterFactory<ProgrammedExporterConfig> {

        private final Scenario scenario;
        private final AtomicReference<Held> heldRequest;
        private final CountDownLatch heldRequestArmed;

        ProgrammedExporterFactory(
                Scenario scenario, AtomicReference<Held> heldRequest, CountDownLatch heldRequestArmed) {
            this.scenario = scenario;
            this.heldRequest = heldRequest;
            this.heldRequestArmed = heldRequestArmed;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.DEVELOPMENT;
        }

        @Override
        public ProgrammedExporterConfig defaultConfig() {
            return ProgrammedExporterConfig.INSTANCE;
        }

        @Override
        public String type() {
            return "programmed";
        }

        @Override
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, ProgrammedExporterConfig config, SignalDescriptor<T> signal) {
            var provenance = new Provenance(ctx.id(), ComponentKind.EXPORTER, signal);
            return new ProgrammedExporter<>(provenance, scenario, heldRequest, heldRequestArmed);
        }
    }

    /// The programmed exporter component. On consume it either holds the first admitted request open
    /// as an incomplete delivery stage, keeping the receiver's admission permit so a second request is
    /// refused, or completes immediately with the scenario's programmed outcome. A `ThrowRuntime`
    /// outcome throws synchronously, which the graph node boundary normalizes to a permanent rejection,
    /// the same wire status the old thrown-from-sink path produced.
    ///
    /// @param <T> the payload type this exporter facet terminates
    private static final class ProgrammedExporter<T extends TelemetryPayload> implements Exporter<T> {

        private final Provenance provenance;
        private final Scenario scenario;
        private final AtomicReference<Held> heldRequest;
        private final CountDownLatch heldRequestArmed;

        ProgrammedExporter(
                Provenance provenance,
                Scenario scenario,
                AtomicReference<Held> heldRequest,
                CountDownLatch heldRequestArmed) {
            this.provenance = provenance;
            this.scenario = scenario;
            this.heldRequest = heldRequest;
            this.heldRequestArmed = heldRequestArmed;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            if (scenario.holdFirstRequest()) {
                var future = new CompletableFuture<DeliveryResult>();
                if (heldRequest.compareAndSet(null, new Held(future, provenance))) {
                    // Hold the first admitted request open so it keeps its admission permit while the
                    // test sends a second request that must be refused. The test releases it later.
                    // Signal that the permit is now held so a probe cannot race ahead of it and win
                    // the single permit itself.
                    heldRequestArmed.countDown();
                    return future;
                }
            }
            return CompletableFuture.completedFuture(toResult(scenario.outcome(), provenance));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
