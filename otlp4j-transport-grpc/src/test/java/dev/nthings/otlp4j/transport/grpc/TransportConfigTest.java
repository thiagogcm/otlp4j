package dev.nthings.otlp4j.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests that the OTLP/gRPC transport honours the full [ClientConfig]/
/// [ServerConfig] surface: headers, compression, retry, and TLS.
///
/// Header/compression/retry tests point an exporter at a bare gRPC server with a capturing or
/// flaky interceptor; TLS is exercised end-to-end through the real receiver and exporter.
@Timeout(30)
@DisplayName("OTLP/gRPC transport config")
class TransportConfigTest {

    private static final Metadata.Key<String> API_KEY =
            Metadata.Key.of("x-otlp-api-key", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> GRPC_ENCODING =
            Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<Receiver> receivers = new ArrayList<>();
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

    @DisplayName("Attaches configured headers to the request")
    @Test
    void attachesConfiguredHeaders() {
        var capture = new CapturingInterceptor();
        var port = startRawServer(capture);
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", port)
                .addHeader("x-otlp-api-key", "secret")
                .build());

        var result = exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(capture.headers.get().get(API_KEY)).isEqualTo("secret");
    }

    @DisplayName("Requests gzip compression when configured")
    @Test
    void requestsGzipCompressionWhenConfigured() {
        var capture = new CapturingInterceptor();
        var port = startRawServer(capture);
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", port)
                .setCompression(Compression.GZIP)
                .build());

        exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(capture.headers.get().get(GRPC_ENCODING)).isEqualTo("gzip");
    }

    @DisplayName("Does not compress by default")
    @Test
    void doesNotCompressByDefault() {
        var capture = new CapturingInterceptor();
        var port = startRawServer(capture);
        var exporter = exporter(ClientConfig.builder().setEndpoint("localhost", port).build());

        exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(capture.headers.get().get(GRPC_ENCODING)).isNotEqualTo("gzip");
    }

    @DisplayName("Retries retryable server errors until success")
    @Test
    void retriesRetryableServerErrors() {
        var flaky = new FlakyInterceptor(2); // first two attempts fail, third succeeds
        var port = startRawServer(flaky);
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", port)
                .setTimeout(Duration.ofSeconds(10))
                .setRetryPolicy(new RetryPolicy(3, Duration.ofMillis(50), Duration.ofMillis(200), 2.0))
                .build());

        var result = exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(flaky.calls.get()).isEqualTo(3);
    }

    @DisplayName("Does not retry when no RetryPolicy is set")
    @Test
    void doesNotRetryWhenPolicyDisabled() {
        var flaky = new FlakyInterceptor(1); // fails the only attempt
        var port = startRawServer(flaky);
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", port)
                .setTimeout(Duration.ofSeconds(5))
                .build());

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
        assertThat(flaky.calls.get()).isEqualTo(1);
    }

    @DisplayName("Round-trips TracesData over custom TLS")
    @Test
    void roundTripsOverTls() {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(serverTls(), OtlpGrpcReceiver.builder().onTraces(t -> {
            received.set(t);
            return ConsumeResult.acceptedStage();
        }));
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(resource("/tls/server.crt")))
                .setTimeout(Duration.ofSeconds(5))
                .build());

        var sent = traces();
        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Plaintext client fails against a TLS server")
    @Test
    void plaintextClientFailsAgainstTlsServer() {
        var receiver = startReceiver(serverTls(),
                OtlpGrpcReceiver.builder().onTraces(t -> ConsumeResult.acceptedStage()));
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(3))
                .build());

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("Builds a client exporter with SystemTrust TLS")
    @Test
    void buildsClientWithSystemTrustTls() {
        // SystemTrust uses the JVM default trust store; the channel connects lazily, so building
        // (and closing) the exporter is enough to exercise the credential path.
        try (var exporter = OtlpGrpcExporter.builder()
                .transport(ClientConfig.builder()
                        .setEndpoint("localhost", 4317)
                        .setTls(Tls.systemTrust())
                        .build())
                .build()) {
            assertThat(exporter).isNotNull();
        }
    }

    @DisplayName("Rejects SystemTrust TLS for a server")
    @Test
    void rejectsSystemTrustForServer() {
        var receiver = OtlpGrpcReceiver.builder()
                .transport(ServerConfig.builder().setPort(0).setTls(Tls.systemTrust()).build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SystemTrust");
    }

    @DisplayName("OtlpGrpcReceiver.on(port) builds a startable plaintext receiver")
    @Test
    void onPortBuildsStartablePlaintextReceiver() {
        var receiver = OtlpGrpcReceiver.on(0);
        receivers.add(receiver);
        receiver.start();
        closeables.add(receiver.traces().discard());

        assertThat(receiver.port()).isPositive();
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(3))
                .build());

        var result = exporter.traces().consume(traces()).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("Builder keeps TLS when port() is set after transport()")
    @Test
    void builderKeepsTlsWhenPortSetAfterTransport() {
        var receiver = OtlpGrpcReceiver.builder()
                .transport(serverTls())
                .setPort(0)
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);
        receiver.start();

        // The server is still TLS, so a plaintext client must fail to connect.
        var plaintext = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(3))
                .build());
        assertThatThrownBy(() -> plaintext.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("A specific bindHost binds that interface only (e.g. loopback) and starts")
    @Test
    void specificBindHostBindsThatInterface() {
        var receiver = OtlpGrpcReceiver.builder()
                .transport(ServerConfig.builder().setBindHost("127.0.0.1").setPort(0).build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        receiver.start();
        assertThat(receiver.port()).isPositive();
    }

    @DisplayName("Half-specified client mutual-TLS material is rejected instead of connecting anonymously")
    @Test
    void halfSpecifiedClientMtlsIsRejected() {
        var config = ClientConfig.builder()
                .setEndpoint("localhost", 4317)
                .setTls(Tls.custom(resource("/tls/server.crt"), null, null))
                .build();

        assertThatThrownBy(() -> exporter(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutual-TLS");
    }

    private static ServerConfig serverTls() {
        // Default loopback bind; a specific bind host binds that interface only (see
        // specificBindHostBindsThatInterface).
        return ServerConfig.builder()
                .setPort(0)
                .setTls(Tls.custom(resource("/tls/server.crt"), resource("/tls/server.key"), null))
                .build();
    }

    private Receiver startReceiver(ServerConfig config, OtlpGrpcReceiver.Builder builder) {
        var receiver = builder.transport(config).build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpExporter exporter(ClientConfig config) {
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

    private static TracesData traces() {
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
