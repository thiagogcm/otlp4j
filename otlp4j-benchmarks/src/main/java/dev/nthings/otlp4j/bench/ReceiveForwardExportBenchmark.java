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
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.ModelBatchPayload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.common.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcExporterFactory;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiverFactory;
import dev.nthings.otlp4j.transport.http.OtlpHttpExporterFactory;
import dev.nthings.otlp4j.transport.http.OtlpHttpReceiverFactory;
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
/// graph, and encode on export, across two real OTLP hops. It parameterizes on transport protocol
/// (gRPC and HTTP) and payload size (small and large), so the baseline captures the four combinations
/// that matter for the Phase 2 efficiency claims. Run with the `gc` profiler enabled (`-prof gc`) to
/// record the allocation rate and bytes-per-operation half of the deliverable.
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class ReceiveForwardExportBenchmark {

    private static final ComponentId RECV = ComponentId.parse("in");
    private static final ComponentId SINK = ComponentId.parse("out");
    private static final ComponentId CLIENT = ComponentId.parse("client");

    /// The transport both hops use.
    @Param({"GRPC", "HTTP"})
    public String protocol;

    /// The payload size label; see [BenchmarkPayloads#itemCount(String)].
    @Param({"small", "large"})
    public String size;

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
        payload = new ModelBatchPayload<>(TRACES, BenchmarkPayloads.traces(BenchmarkPayloads.itemCount(size)));

        backend = buildBackend(protocol);
        backend.start();
        var backendPort = boundPort(backend);

        gateway = buildGateway(protocol, backendPort);
        gateway.start();
        var gatewayPort = boundPort(gateway);

        client = buildClient(protocol, gatewayPort);
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

    private static GatewayRuntime buildBackend(String protocol) {
        return Gateway.builder()
                .receiver(RECV, receiverFactory(protocol), ephemeralReceiver(protocol))
                .exporter(SINK, new AcceptingExporterFactory(), EmptyConfig.INSTANCE)
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(), List.of(SINK)))
                .build();
    }

    private static GatewayRuntime buildGateway(String protocol, int backendPort) {
        return Gateway.builder()
                .receiver(RECV, receiverFactory(protocol), ephemeralReceiver(protocol))
                .exporter(SINK, exporterFactory(protocol), OtlpExporterConfig.defaults(backendPort))
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(), List.of(SINK)))
                .build();
    }

    private Exporter<TelemetryPayload> buildClient(String protocol, int gatewayPort) {
        var ctx = new CreateContext(CLIENT, base);
        return exporterFactory(protocol).create(ctx, OtlpExporterConfig.defaults(gatewayPort), tracesDescriptor());
    }

    private static ReceiverFactory<OtlpReceiverConfig> receiverFactory(String protocol) {
        return switch (protocol) {
            case "GRPC" -> new OtlpGrpcReceiverFactory();
            case "HTTP" -> new OtlpHttpReceiverFactory();
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }

    private static ExporterFactory<OtlpExporterConfig> exporterFactory(String protocol) {
        return switch (protocol) {
            case "GRPC" -> new OtlpGrpcExporterFactory();
            case "HTTP" -> new OtlpHttpExporterFactory();
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }

    private static OtlpReceiverConfig ephemeralReceiver(String protocol) {
        var port = "GRPC".equals(protocol)
                ? OtlpGrpcReceiverFactory.DEFAULT_PORT
                : OtlpHttpReceiverFactory.DEFAULT_PORT;
        return OtlpReceiverConfig.defaults(port).withEphemeralPort();
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
        public StabilityLevel stability() {
            return StabilityLevel.DEVELOPMENT;
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
}
