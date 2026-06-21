package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.exporter.OtlpGrpcExporter;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.receiver.OtlpGrpcReceiver;
import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.Compression;
import dev.nthings.otlp4j.spi.RetryPolicy;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import dev.nthings.otlp4j.spi.Tls;
import dev.nthings.otlp4j.testing.Fixtures;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests that the OTLP/gRPC transport honours the full [ClientTransportConfig]/
/// [ServerTransportConfig] surface: headers, compression, retry, and TLS.
///
/// Header/compression/retry tests point an exporter at a bare gRPC server with a capturing or
/// flaky interceptor; TLS is exercised end-to-end through the real receiver and exporter.
@Timeout(30)
class TransportConfigTest {

    private static final Metadata.Key<String> API_KEY =
            Metadata.Key.of("x-otlp-api-key", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> GRPC_ENCODING =
            Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<OtlpGrpcReceiver> receivers = new ArrayList<>();
    private Server rawServer;

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception _) { /* keep tearing down */ }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) { /* keep tearing down */ }
        }
        if (rawServer != null) {
            rawServer.shutdownNow();
            try {
                rawServer.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void attachesConfiguredHeaders() {
        var capture = new CapturingInterceptor();
        int port = startRawServer(capture);
        var exporter = exporter(ClientTransportConfig.builder()
                .endpoint("localhost", port)
                .header("x-otlp-api-key", "secret")
                .build());

        var result = exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(capture.headers.get().get(API_KEY)).isEqualTo("secret");
    }

    @Test
    void requestsGzipCompressionWhenConfigured() {
        var capture = new CapturingInterceptor();
        int port = startRawServer(capture);
        var exporter = exporter(ClientTransportConfig.builder()
                .endpoint("localhost", port)
                .compression(Compression.GZIP)
                .build());

        exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(capture.headers.get().get(GRPC_ENCODING)).isEqualTo("gzip");
    }

    @Test
    void doesNotCompressByDefault() {
        var capture = new CapturingInterceptor();
        int port = startRawServer(capture);
        var exporter = exporter(ClientTransportConfig.builder().endpoint("localhost", port).build());

        exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(capture.headers.get().get(GRPC_ENCODING)).isNotEqualTo("gzip");
    }

    @Test
    void retriesRetryableServerErrors() {
        var flaky = new FlakyInterceptor(2); // first two attempts fail, third succeeds
        int port = startRawServer(flaky);
        var exporter = exporter(ClientTransportConfig.builder()
                .endpoint("localhost", port)
                .timeout(Duration.ofSeconds(10))
                .retry(new RetryPolicy(3, Duration.ofMillis(50), Duration.ofMillis(200)))
                .build());

        var result = exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(flaky.calls.get()).isEqualTo(3);
    }

    @Test
    void doesNotRetryWhenPolicyDisabled() {
        var flaky = new FlakyInterceptor(1); // fails the only attempt
        int port = startRawServer(flaky);
        var exporter = exporter(ClientTransportConfig.builder()
                .endpoint("localhost", port)
                .timeout(Duration.ofSeconds(5))
                .build());

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
        assertThat(flaky.calls.get()).isEqualTo(1);
    }

    @Test
    void roundTripsOverTls() {
        var received = new AtomicReference<TraceData>();
        var receiver = startReceiver(serverTls(), OtlpGrpcReceiver.builder().onTraces(t -> {
            received.set(t);
            return ConsumeResult.acceptedStage();
        }));
        var exporter = exporter(ClientTransportConfig.builder()
                .endpoint("localhost", receiver.port())
                .tls(new Tls.Custom(null, null, resource("/tls/server.crt")))
                .timeout(Duration.ofSeconds(5))
                .build());

        var sent = traces();
        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @Test
    void plaintextClientFailsAgainstTlsServer() {
        var receiver = startReceiver(serverTls(),
                OtlpGrpcReceiver.builder().onTraces(t -> ConsumeResult.acceptedStage()));
        var exporter = exporter(ClientTransportConfig.builder()
                .endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(3))
                .build());

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @Test
    void buildsClientWithSystemTrustTls() {
        // SystemTrust uses the JVM default trust store; the channel connects lazily, so building
        // (and closing) the exporter is enough to exercise the credential path.
        try (var exporter = OtlpGrpcExporter.builder()
                .transport(ClientTransportConfig.builder()
                        .endpoint("localhost", 4317)
                        .tls(Tls.SystemTrust.instance())
                        .build())
                .build()) {
            assertThat(exporter).isNotNull();
        }
    }

    @Test
    void rejectsSystemTrustForServer() {
        var receiver = OtlpGrpcReceiver.builder()
                .transport(ServerTransportConfig.builder().port(0).tls(Tls.SystemTrust.instance()).build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SystemTrust");
    }

    private static ServerTransportConfig serverTls() {
        return ServerTransportConfig.builder()
                .bindHost("localhost")
                .port(0)
                .tls(new Tls.Custom(resource("/tls/server.crt"), resource("/tls/server.key"), null))
                .build();
    }

    private OtlpGrpcReceiver startReceiver(ServerTransportConfig config, OtlpGrpcReceiver.Builder builder) {
        var receiver = builder.transport(config).build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpGrpcExporter exporter(ClientTransportConfig config) {
        var exporter = OtlpGrpcExporter.builder().transport(config).build();
        closeables.add(exporter);
        return exporter;
    }

    /// Starts a bare gRPC server serving only TraceService, wrapped in `interceptor`.
    private int startRawServer(ServerInterceptor interceptor) {
        try {
            rawServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                    .addService(ServerInterceptors.intercept(new StubTraceService(), interceptor))
                    .build()
                    .start();
            return rawServer.getPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static TraceData traces() {
        return Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER));
    }

    private static Path resource(String name) {
        try {
            return Path.of(TransportConfigTest.class.getResource(name).toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /// Trivial TraceService that always acks with an empty response.
    private static final class StubTraceService extends TraceServiceGrpc.TraceServiceImplBase {
        @Override
        public void export(
                ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> observer) {
            observer.onNext(ExportTraceServiceResponse.getDefaultInstance());
            observer.onCompleted();
        }
    }

    /// Captures the metadata of the most recent call.
    private static final class CapturingInterceptor implements ServerInterceptor {
        final AtomicReference<Metadata> headers = new AtomicReference<>();

        @Override
        public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
            this.headers.set(headers);
            return next.startCall(call, headers);
        }
    }

    /// Fails the first `failFirst` calls with UNAVAILABLE; serves the rest normally.
    private static final class FlakyInterceptor implements ServerInterceptor {
        final AtomicInteger calls = new AtomicInteger();
        private final int failFirst;

        FlakyInterceptor(int failFirst) {
            this.failFirst = failFirst;
        }

        @Override
        public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
            if (calls.incrementAndGet() <= failFirst) {
                call.close(Status.UNAVAILABLE.withDescription("flaky"), new Metadata());
                return new ServerCall.Listener<>() {};
            }
            return next.startCall(call, headers);
        }
    }
}
