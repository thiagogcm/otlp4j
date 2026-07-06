package dev.nthings.otlp4j.bench;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.OtlpExporter;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.PipelineHandle;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcExporter;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiver;
import dev.nthings.otlp4j.transport.http.OtlpHttpExporter;
import dev.nthings.otlp4j.transport.http.OtlpHttpReceiver;
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

/// End-to-end receive-forward-export baseline over the real transports.
///
/// One measured operation exports a traces batch from a client to a gateway receiver, whose pipeline
/// forwards it through an exporter to a backend receiver, and blocks until the backend has
/// acknowledged. The path therefore exercises decode on receive, the forwarding pipeline, and encode
/// on export, across two real OTLP hops. It parameterizes on transport protocol (gRPC and HTTP) and
/// payload size (small and large), so the baseline captures the four combinations that matter for the
/// Phase 2 efficiency claims. Run with the `gc` profiler enabled (`-prof gc`) to record the
/// allocation rate and bytes-per-operation half of the deliverable.
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class ReceiveForwardExportBenchmark {

    /// The transport both hops use.
    @Param({"GRPC", "HTTP"})
    public String protocol;

    /// The payload size label; see [BenchmarkPayloads#itemCount(String)].
    @Param({"small", "large"})
    public String size;

    private Receiver backend;
    private Receiver gateway;
    private OtlpExporter forwarder;
    private OtlpExporter client;
    private PipelineHandle subscription;
    private TracesData payload;

    /// Stands up the backend receiver, the forwarding exporter, the gateway receiver with its
    /// forwarding pipeline, and the client exporter, all on ephemeral loopback ports for the chosen
    /// transport, then pre-builds the payload for the chosen size.
    @Setup(Level.Trial)
    public void setUp() {
        payload = BenchmarkPayloads.traces(BenchmarkPayloads.itemCount(size));

        backend = acceptingReceiver(protocol);
        forwarder = exporter(protocol, backend.port());

        gateway = bareReceiver(protocol);
        subscription = Pipeline.from(gateway.traces()).to(forwarder.traces());
        gateway.start();

        client = exporter(protocol, gateway.port());
    }

    /// Exports one batch end-to-end and blocks until the backend acknowledges.
    ///
    /// @return the client-observed delivery result
    @Benchmark
    public ConsumeResult receiveForwardExport() {
        return client.traces().consume(payload).toCompletableFuture().join();
    }

    /// Closes every client and server built in [#setUp()], releasing ports and threads.
    @TearDown(Level.Trial)
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (subscription != null) {
            subscription.close();
        }
        if (gateway != null) {
            gateway.shutdownNow().toCompletableFuture().join();
        }
        if (forwarder != null) {
            forwarder.close();
        }
        if (backend != null) {
            backend.shutdownNow().toCompletableFuture().join();
        }
    }

    private static Receiver acceptingReceiver(String protocol) {
        return switch (protocol) {
            case "GRPC" -> OtlpGrpcReceiver.builder()
                    .ephemeralPort()
                    .onTraces(batch -> ConsumeResult.acceptedStage())
                    .build()
                    .start();
            case "HTTP" -> OtlpHttpReceiver.builder()
                    .ephemeralPort()
                    .onTraces(batch -> ConsumeResult.acceptedStage())
                    .build()
                    .start();
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }

    private static Receiver bareReceiver(String protocol) {
        return switch (protocol) {
            case "GRPC" -> OtlpGrpcReceiver.builder().ephemeralPort().build();
            case "HTTP" -> OtlpHttpReceiver.builder().ephemeralPort().build();
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }

    private static OtlpExporter exporter(String protocol, int port) {
        return switch (protocol) {
            case "GRPC" -> OtlpGrpcExporter.to("127.0.0.1", port);
            case "HTTP" -> OtlpHttpExporter.to("127.0.0.1", port);
            default -> throw new IllegalArgumentException("unknown protocol: " + protocol);
        };
    }
}
