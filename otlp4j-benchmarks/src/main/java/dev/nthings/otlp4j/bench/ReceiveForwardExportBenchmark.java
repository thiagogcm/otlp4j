package dev.nthings.otlp4j.bench;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
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

/// End-to-end receive-forward-export benchmark over the service graph runtime.
///
/// One measured operation exports a traces batch from a client exporter to a gateway receiver, whose
/// single-pipeline graph forwards it through an OTLP exporter to a backend receiver, and blocks until
/// the backend has acknowledged. The path exercises decode on receive, graph dispatch, and encode on
/// export across two real OTLP hops. Protocol and immutable domain batch size are the only parameters.
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

    /// The wire protocol both hops use.
    @Param({"GRPC", "HTTP"})
    public String protocol;

    /// The payload size label; see [BenchmarkPayloads#itemCount(String)].
    @Param({"small", "large"})
    public String size;

    private ComponentContext base;
    private ConsumeContext context;
    private GatewayRuntime backend;
    private GatewayRuntime gateway;
    private Exporter<TracesData> client;
    private Payload<TracesData> payload;

    /// Stands up the backend graph (a receiver feeding an accepting exporter), the gateway graph (a
    /// receiver forwarding through an OTLP exporter to the backend), and a standalone client exporter
    /// pointed at the gateway on an ephemeral loopback port, then pre-builds the immutable payload
    /// for the chosen size.
    @Setup(Level.Trial)
    public void setUp() {
        base = new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
        context = new BenchContext(base.clock());
        payload = Payload.of(TRACES, BenchmarkPayloads.traces(BenchmarkPayloads.itemCount(size)));

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
        var gw = Gateway.builder();
        gw.receiver(RECV, receiverFactory(protocol), ephemeralReceiver(protocol));
        // The backend's terminal exporter accepts every batch immediately, standing in for a backend
        // store while keeping the forwarding path fully exercised.
        gw.exporter(SINK, ExporterFactory.of("accept", (ctx, data, provenance) -> DeliveryResult.acceptedStage(provenance)));
        gw.pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(), List.of(SINK)));
        return gw.build();
    }

    private static GatewayRuntime buildGateway(String protocol, int backendPort) {
        var gw = Gateway.builder();
        gw.receiver(RECV, receiverFactory(protocol), ephemeralReceiver(protocol));
        gw.exporter(SINK, exporterFactory(protocol), OtlpExporterConfig.defaults(backendPort));
        gw.pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(), List.of(SINK)));
        return gw.build();
    }

    private Exporter<TracesData> buildClient(String protocol, int gatewayPort) {
        var ctx = new CreateContext(CLIENT, base);
        return exporterFactory(protocol).create(ctx, OtlpExporterConfig.defaults(gatewayPort), TRACES);
    }

    /// Selects the consolidated OTLP receiver for the chosen wire protocol.
    private static ReceiverFactory<OtlpReceiverConfig> receiverFactory(String protocol) {
        return switch (protocol) {
            case "GRPC" -> OtlpReceiverFactory.grpc();
            case "HTTP" -> OtlpReceiverFactory.http();
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }

    /// Selects the consolidated OTLP exporter for the chosen wire protocol.
    private static ExporterFactory<OtlpExporterConfig> exporterFactory(String protocol) {
        return switch (protocol) {
            case "GRPC" -> OtlpExporterFactory.grpc();
            case "HTTP" -> OtlpExporterFactory.http();
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }

    /// The chosen factory's default config carries the conventional port for the protocol and is
    /// rebound to an ephemeral one for the benchmark.
    private static OtlpReceiverConfig ephemeralReceiver(String protocol) {
        return receiverFactory(protocol).defaultConfig().withEphemeralPort();
    }

    private static int boundPort(GatewayRuntime runtime) {
        return runtime.boundPort(RECV).orElseThrow(() -> new IllegalStateException("receiver has no bound port"));
    }

    // ---- fixtures --------------------------------------------------------------------------------

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
}
