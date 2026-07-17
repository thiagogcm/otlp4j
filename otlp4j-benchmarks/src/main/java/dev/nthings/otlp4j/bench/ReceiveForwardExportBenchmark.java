package dev.nthings.otlp4j.bench;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.common.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.otlp.OtlpArmeriaGrpcExporterFactory;
import dev.nthings.otlp4j.transport.otlp.OtlpArmeriaGrpcReceiverFactory;
import dev.nthings.otlp4j.transport.otlp.OtlpArmeriaHttpExporterFactory;
import dev.nthings.otlp4j.transport.otlp.OtlpArmeriaHttpReceiverFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/// End-to-end receive-forward-export baseline over the real transports on the service graph runtime.
///
/// One measured operation exports a traces batch from a client exporter to a gateway receiver, whose
/// single-pipeline graph forwards it through an OTLP exporter to a backend receiver, and blocks until
/// the backend has acknowledged. The path therefore exercises decode on receive, the forwarding
/// graph, and encode on export, across two real OTLP hops. It parameterizes on transport backend
/// (`ARMERIA`, the single production transport now that the legacy transports are deleted), transport
/// protocol (gRPC and HTTP), payload size (small and large), and pipeline mode (`passthrough` and
/// `domainview`), so the baseline captures the combinations that matter for the Phase 2 efficiency
/// claims and the Phase 4 transport parity gate. Run with the `gc` profiler enabled (`-prof gc`) to
/// record the allocation rate and bytes-per-operation half of the deliverable.
///
/// The `transport` parameter recorded the Phase 4 parity evidence. It now has the single value
/// `ARMERIA`, which wires the consolidated `otlp4j-transport-otlp` receivers and exporters; the
/// committed legacy baseline JSON is retained for history. Both hops (the gateway receiver and the
/// OTLP exporter it forwards through, plus the standalone client exporter) use the same backend within
/// one measured operation.
///
/// The `mode` parameter is the Phase 2 pass-through evidence. The gateway hop's receiver always
/// decodes the wire bytes into a proto request and wraps it as a proto-backed payload
/// (`SignalCodec.<signal>.proto(request)`), never a domain record, so `passthrough` (an empty
/// processor list) forwards that proto-backed payload straight to the OTLP exporter without ever
/// building a `TracesData`. `domainview` inserts one domain-view processor into the same gateway
/// pipeline; it reads the payload through the interface-level `DOMAIN_IMMUTABLE` view, which forces
/// exactly one `toDomain`, and re-emits a fresh domain-backed payload, which forces exactly one
/// `toProto` when the OTLP exporter serialises it. Comparing `passthrough` against `domainview` on
/// the same protocol and size isolates the allocation the payload redesign removes from a pipeline
/// that never touches the domain.
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class ReceiveForwardExportBenchmark {

    private static final ComponentId RECV = ComponentId.parse("in");
    private static final ComponentId PROC = ComponentId.parse("proc");
    private static final ComponentId SINK = ComponentId.parse("out");
    private static final ComponentId CLIENT = ComponentId.parse("client");

    /// The transport implementation both hops use. Now that the legacy transports are deleted this
    /// has a single value, the consolidated Armeria transport; the committed legacy baseline JSON is
    /// kept for history. See the class Javadoc for what this isolates.
    @Param({"ARMERIA"})
    public String transport;

    /// The wire protocol both hops use.
    @Param({"GRPC", "HTTP"})
    public String protocol;

    /// The payload size label; see [BenchmarkPayloads#itemCount(String)].
    @Param({"small", "large"})
    public String size;

    /// The gateway pipeline shape. `passthrough` wires no processor, so the gateway hop forwards the
    /// proto-backed payload the receiver decoded straight to the OTLP exporter. `domainview` wires one
    /// domain-view processor, forcing exactly one `toDomain` on the way in and one `toProto` on the
    /// way out. See the class Javadoc for what this isolates.
    @Param({"passthrough", "domainview"})
    public String mode;

    private ComponentContext base;
    private ConsumeContext context;
    private GatewayRuntime backend;
    private GatewayRuntime gateway;
    private Exporter<TelemetryPayload> client;
    private TelemetryPayload payload;

    /// Stands up the backend graph (a receiver feeding an accepting exporter), the gateway graph (a
    /// receiver forwarding through an OTLP exporter to the backend), and a standalone client exporter
    /// pointed at the gateway, all on ephemeral loopback ports for the chosen transport, then
    /// pre-builds the payload for the chosen size.
    @Setup(Level.Trial)
    public void setUp() {
        base = new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
        context = new BenchContext(base.clock());
        payload = Payloads.domain(TRACES, BenchmarkPayloads.traces(BenchmarkPayloads.itemCount(size)));

        backend = buildBackend(transport, protocol);
        backend.start();
        var backendPort = boundPort(backend);

        gateway = buildGateway(transport, protocol, backendPort, mode);
        gateway.start();
        var gatewayPort = boundPort(gateway);

        client = buildClient(transport, protocol, gatewayPort);
        client.start(base).toCompletableFuture().join();
    }

    /// Exports one batch end-to-end and blocks until the backend acknowledges.
    ///
    /// @return the client-observed delivery result
    @Benchmark
    public DeliveryResult receiveForwardExport() {
        return client.consume(context, payload).toCompletableFuture().join();
    }

    /// Closes the client, the gateway, and the backend built in [#setUp()], releasing ports and
    /// threads.
    @TearDown(Level.Trial)
    public void tearDown() {
        if (client != null) {
            client.shutdown(new ShutdownContext(Duration.ofSeconds(5))).toCompletableFuture().join();
        }
        if (gateway != null) {
            gateway.close();
        }
        if (backend != null) {
            backend.close();
        }
    }

    // ---- graph assembly --------------------------------------------------------------------------

    private static GatewayRuntime buildBackend(String transport, String protocol) {
        return Gateway.builder()
                .receiver(RECV, receiverFactory(transport, protocol), ephemeralReceiver(transport, protocol))
                .exporter(SINK, new AcceptingExporterFactory(), EmptyConfig.INSTANCE)
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(), List.of(SINK)))
                .build();
    }

    private static GatewayRuntime buildGateway(String transport, String protocol, int backendPort, String mode) {
        var assembler = Gateway.builder()
                .receiver(RECV, receiverFactory(transport, protocol), ephemeralReceiver(transport, protocol))
                .exporter(SINK, exporterFactory(transport, protocol), OtlpExporterConfig.defaults(backendPort));
        List<ComponentId> processors;
        switch (mode) {
            case "passthrough" -> processors = List.of();
            case "domainview" -> {
                assembler.processor(PROC, new DomainViewProcessorFactory(), EmptyConfig.INSTANCE);
                processors = List.of(PROC);
            }
            default -> throw new IllegalArgumentException("unknown mode: " + mode);
        }
        return assembler
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), processors, List.of(SINK)))
                .build();
    }

    private Exporter<TelemetryPayload> buildClient(String transport, String protocol, int gatewayPort) {
        var ctx = new CreateContext(CLIENT, base);
        return exporterFactory(transport, protocol)
                .create(ctx, OtlpExporterConfig.defaults(gatewayPort), tracesDescriptor());
    }

    /// Selects the receiver factory for a `transport` x `protocol` cell. `ARMERIA` selects the
    /// consolidated `otlp4j-transport-otlp` factories, the single production transport.
    private static ReceiverFactory<OtlpReceiverConfig> receiverFactory(String transport, String protocol) {
        return switch (transport) {
            case "ARMERIA" -> switch (protocol) {
                case "GRPC" -> new OtlpArmeriaGrpcReceiverFactory();
                case "HTTP" -> new OtlpArmeriaHttpReceiverFactory();
                default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
            };
            default -> throw new IllegalArgumentException("unknown transport: " + transport);
        };
    }

    /// Selects the exporter factory for a `transport` x `protocol` cell. See [#receiverFactory].
    private static ExporterFactory<OtlpExporterConfig> exporterFactory(String transport, String protocol) {
        return switch (transport) {
            case "ARMERIA" -> switch (protocol) {
                case "GRPC" -> new OtlpArmeriaGrpcExporterFactory();
                case "HTTP" -> new OtlpArmeriaHttpExporterFactory();
                default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
            };
            default -> throw new IllegalArgumentException("unknown transport: " + transport);
        };
    }

    /// The chosen factory's own [ReceiverFactory#defaultConfig()] already carries the right
    /// conventional port for the `transport` x `protocol` cell (4317 for gRPC, 4318 for HTTP), so it
    /// only needs to be rebound to an ephemeral one for the benchmark.
    private static OtlpReceiverConfig ephemeralReceiver(String transport, String protocol) {
        return receiverFactory(transport, protocol).defaultConfig().withEphemeralPort();
    }

    private static int boundPort(GatewayRuntime runtime) {
        return runtime.component(RECV)
                .filter(NetworkBound.class::isInstance)
                .map(NetworkBound.class::cast)
                .map(NetworkBound::port)
                .orElseThrow(() -> new IllegalStateException("receiver has no bound port"));
    }

    @SuppressWarnings("unchecked")
    private static SignalDescriptor<TelemetryPayload> tracesDescriptor() {
        return (SignalDescriptor<TelemetryPayload>) (SignalDescriptor<?>) TRACES;
    }

    // ---- fixtures --------------------------------------------------------------------------------

    private record EmptyConfig() implements ComponentConfig {
        static final EmptyConfig INSTANCE = new EmptyConfig();
    }

    private static final class BenchContext implements ConsumeContext {
        private final Deadline deadline;

        BenchContext(Clock clock) {
            this.deadline = Deadline.after(Duration.ofSeconds(30), clock);
        }

        @Override
        public ClientMetadata metadata() {
            return ClientMetadata.empty();
        }

        @Override
        public Deadline deadline() {
            return deadline;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void onCancel(Runnable callback) {
            // never cancelled in the benchmark
        }
    }

    /// The backend's terminal exporter: it accepts every batch immediately, standing in for a backend
    /// store while keeping the forwarding path fully exercised.
    private static final class AcceptingExporterFactory implements ExporterFactory<EmptyConfig> {

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
        }

        @Override
        public EmptyConfig defaultConfig() {
            return EmptyConfig.INSTANCE;
        }

        @Override
        public String type() {
            return "accept";
        }

        @Override
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, EmptyConfig config, SignalDescriptor<T> signal) {
            return new AcceptingExporter<>(new Provenance(ctx.id(), ComponentKind.EXPORTER, signal));
        }
    }

    private static final class AcceptingExporter<T extends TelemetryPayload> implements Exporter<T> {

        private final Provenance provenance;

        AcceptingExporter(Provenance provenance) {
            this.provenance = provenance;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext ctx, T data) {
            return DeliveryResult.acceptedStage(provenance);
        }

        @Override
        public CompletionStage<Void> start(ComponentContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext ctx) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /// The `domainview` mode's one processor: it reads the payload through the interface-level
    /// `DOMAIN_IMMUTABLE` view and re-emits the same batch as a fresh domain-backed payload. This is
    /// the same shape as the seed `FunctionProcessorFactory` in `otlp4j-samples` with an identity
    /// transform, kept as a local fixture here so the benchmark module does not take on a samples
    /// dependency. It is hardcoded to traces because the benchmark wires only a traces pipeline.
    private static final class DomainViewProcessorFactory implements ProcessorFactory<EmptyConfig> {

        private final SignalDescriptor<TracesData> signal = TRACES;

        @Override
        public ComponentKind kind() {
            return ComponentKind.PROCESSOR;
        }

        @Override
        public EmptyConfig defaultConfig() {
            return EmptyConfig.INSTANCE;
        }

        @Override
        public String type() {
            return "domain-view";
        }

        @Override
        public <T extends TelemetryPayload> Processor<T> create(
                CreateContext ctx, EmptyConfig config, SignalDescriptor<T> reqSignal, SignalConsumer<T> next) {
            if (!reqSignal.equals(signal)) {
                throw new IllegalArgumentException(
                        "domain-view processor operates on " + signal + " but was created for " + reqSignal);
            }
            var processor = new DomainViewProcessor(signal, payloadConsumer(next));
            return castProcessor(processor);
        }

        @SuppressWarnings("unchecked")
        private static SignalConsumer<TelemetryPayload> payloadConsumer(SignalConsumer<?> next) {
            return (SignalConsumer<TelemetryPayload>) next;
        }

        @SuppressWarnings("unchecked")
        private static <T extends TelemetryPayload> Processor<T> castProcessor(Processor<TelemetryPayload> processor) {
            return (Processor<T>) (Processor<?>) processor;
        }
    }

    private static final class DomainViewProcessor implements Processor<TelemetryPayload> {

        private final SignalDescriptor<TracesData> signal;

        private final SignalConsumer<TelemetryPayload> next;

        DomainViewProcessor(SignalDescriptor<TracesData> signal, SignalConsumer<TelemetryPayload> next) {
            this.signal = signal;
            this.next = next;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext ctx, TelemetryPayload data) {
            var batch = signal.batchType().cast(data.view(PayloadView.DOMAIN_IMMUTABLE));
            return next.consume(ctx, Payloads.domain(signal, batch));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext ctx) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
