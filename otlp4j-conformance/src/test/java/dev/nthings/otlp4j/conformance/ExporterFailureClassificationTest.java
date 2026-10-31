package dev.nthings.otlp4j.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.rpc.RetryInfo;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayBuilder;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.policy.ProgrammedOtlpServer;
import dev.nthings.otlp4j.testing.policy.ProgrammedResponse;
import dev.nthings.otlp4j.transport.OtlpExporter;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/// Pins how an exporter reports what the wire did, and how a gateway relays it to its upstream sender.
///
/// The rule under test is the OpenTelemetry one: a sender retries exactly the failures that are
/// retryable (HTTP 429/502/503/504, the retryable gRPC codes, and anything that never reached an
/// answering server), and must not retry the rest. So an exporter must report each wire outcome as
/// a typed [DeliveryResult] that says which kind it was, and a gateway must pass that on rather than
/// flattening every downstream failure into a permanent `500` / `INTERNAL`, which would make its
/// own upstream sender drop data a retry would have delivered.
///
/// Every case drives real servers and clients over loopback.
class ExporterFailureClassificationTest {

    private static final RetryPolicy ONCE = new RetryPolicy(1, BackoffPolicy.defaults(), true);
    private static final RetryPolicy ONCE_IGNORING_HINTS = new RetryPolicy(1, BackoffPolicy.defaults(), false);
    private static final RetryPolicy FAST = new RetryPolicy(
            2, new BackoffPolicy(Duration.ofMillis(10), 1.0, 0.0, Duration.ofMillis(10)), true);

    private static final Metadata.Key<String> PUSHBACK =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<byte[]> DETAILS =
            Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER);

    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            try {
                cleanup.get(i).close();
            } catch (RuntimeException ignored) {
                // Best-effort teardown; the assertions already ran.
            }
        }
    }

    private <C extends AutoCloseable> C own(C closeable) {
        cleanup.add(closeable);
        return closeable;
    }

    // --- fixtures ------------------------------------------------------------------------------

    /// A canned OTLP/HTTP endpoint answering every POST with one fixed response.
    private HttpServer http(int status, String contentType, byte[] body, Map<String, String> headers)
            throws IOException {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> answer(exchange, status, contentType, body, headers));
        server.start();
        cleanup.add(() -> server.stop(0));
        return server;
    }

    private static void answer(
            HttpExchange exchange, int status, String contentType, byte[] body, Map<String, String> headers)
            throws IOException {
        try (exchange) {
            exchange.getRequestBody().readAllBytes();
            if (contentType != null) {
                exchange.getResponseHeaders().add("Content-Type", contentType);
            }
            headers.forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
        }
    }

    private HttpServer http(int status) throws IOException {
        return http(status, null, new byte[0], Map.of());
    }

    /// A canned OTLP/gRPC trace endpoint failing every call with one fixed status, optionally after
    /// the response headers have already gone out (so the status arrives in separate trailers rather
    /// than a trailers-only response).
    private Server grpc(Supplier<StatusRuntimeException> failure, boolean sendHeadersFirst) throws IOException {
        var service = new TraceServiceGrpc.TraceServiceImplBase() {
            @Override
            public void export(
                    ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> observer) {
                observer.onError(failure.get());
            }
        };
        var builder = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create());
        if (sendHeadersFirst) {
            builder.addService(io.grpc.ServerInterceptors.intercept(service, new io.grpc.ServerInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                        io.grpc.ServerCall<ReqT, RespT> call, Metadata headers,
                        io.grpc.ServerCallHandler<ReqT, RespT> next) {
                    call.sendHeaders(new Metadata());
                    return next.startCall(call, headers);
                }
            }));
        } else {
            builder.addService(service);
        }
        var server = builder.build().start();
        cleanup.add(() -> {
            server.shutdownNow();
            server.awaitTermination(2, TimeUnit.SECONDS);
        });
        return server;
    }

    private Server grpc(Supplier<StatusRuntimeException> failure) throws IOException {
        return grpc(failure, false);
    }

    private static StatusRuntimeException status(Status status, long pushbackMs) {
        var trailers = new Metadata();
        trailers.put(PUSHBACK, Long.toString(pushbackMs));
        return status.asRuntimeException(trailers);
    }

    private static StatusRuntimeException withRetryInfo(Status.Code code, Duration delay) {
        var detail = RetryInfo.newBuilder()
                .setRetryDelay(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(delay.getSeconds()).setNanos(delay.getNano()))
                .build();
        var rpcStatus = com.google.rpc.Status.newBuilder()
                .setCode(code.value())
                .setMessage("programmed")
                .addDetails(Any.pack(detail))
                .build();
        var trailers = new Metadata();
        trailers.put(DETAILS, rpcStatus.toByteArray());
        return code.toStatus().withDescription("programmed").asRuntimeException(trailers);
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private OtlpExporter exporter(Protocol protocol, int port, RetryPolicy retry) {
        return exporter(protocol, OtlpExporterConfig.builder(4317).host("127.0.0.1").port(port).retryPolicy(retry)
                .build());
    }

    private OtlpExporter exporter(Protocol protocol, OtlpExporterConfig config) {
        var builder = protocol == Protocol.GRPC ? OtlpExporter.grpcBuilder() : OtlpExporter.httpBuilder();
        return own(builder.config(config).build().start());
    }

    private static DeliveryResult send(OtlpExporter exporter) {
        return exporter.export(SignalDescriptor.TRACES, Fixtures.traceData(Fixtures.span("s", Span.Kind.SERVER)))
                .toCompletableFuture().orTimeout(20, TimeUnit.SECONDS).join();
    }

    private static DeliveryResult.Rejected rejected(DeliveryResult result) {
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        return (DeliveryResult.Rejected) result;
    }

    private static ExportTraceServiceRequest request() {
        return ExportTraceServiceRequest.newBuilder().addResourceSpans(ResourceSpans.getDefaultInstance()).build();
    }

    // --- HTTP exporter -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {429, 502, 503, 504})
    @DisplayName("an HTTP status the OTLP spec says to retry is a retryable downstream rejection")
    void httpRetryableStatus(int status) throws Exception {
        var server = http(status);

        var rejected = rejected(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.downstream()).isTrue();
        assertThat(rejected.message()).contains("HTTP " + status);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 408, 413, 500, 501, 301})
    @DisplayName("any other non-2xx HTTP status is a permanent downstream rejection")
    void httpPermanentStatus(int status) throws Exception {
        var server = http(status);

        var rejected = rejected(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isTrue();
        assertThat(rejected.retryAfter()).isNull();
    }

    @Test
    @DisplayName("a 503 with Retry-After carries the hint when the policy honours server hints")
    void httpRetryAfterHonoured() throws Exception {
        var server = http(503, null, new byte[0], Map.of("Retry-After", "7"));

        var rejected = rejected(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.retryAfter()).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    @DisplayName("a 503 Retry-After is dropped when the policy does not honour server hints")
    void httpRetryAfterIgnored() throws Exception {
        var server = http(503, null, new byte[0], Map.of("Retry-After", "7"));

        var rejected = rejected(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE_IGNORING_HINTS)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.retryAfter()).isNull();
    }

    @Test
    @DisplayName("the rejection keeps the message of the peer's google.rpc.Status body")
    void httpRejectionMessage() throws Exception {
        var body = com.google.rpc.Status.newBuilder().setCode(3).setMessage("bad span").build().toByteArray();
        var server = http(400, "application/x-protobuf", body, Map.of());

        var rejected = rejected(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)));

        assertThat(rejected.message()).contains("HTTP 400").contains("bad span");
    }

    @Test
    @DisplayName("a 2xx with an HTML body is accepted: the status is what the server committed to")
    void httpHtmlBodyIsAccepted() throws Exception {
        var server = http(200, "text/html", "<html>ok</html>".getBytes(), Map.of());

        assertThat(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)))
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @Test
    @DisplayName("a 2xx with no Content-Type and a body is accepted")
    void httpBodyWithoutContentTypeIsAccepted() throws Exception {
        var server = http(200, null, "ok".getBytes(), Map.of());

        assertThat(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)))
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @Test
    @DisplayName("a 2xx with an empty body is accepted")
    void httpEmptyBodyIsAccepted() throws Exception {
        var server = http(200, "application/x-protobuf", new byte[0], Map.of());

        assertThat(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)))
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @Test
    @DisplayName("a 2xx claiming protobuf that does not parse is permanent, never retryable: the data was accepted")
    void httpUnparseableProtobufIsPermanent() throws Exception {
        var server = http(200, "application/x-protobuf", new byte[] {(byte) 0xFF, (byte) 0xFF, 0x01}, Map.of());

        var rejected = rejected(send(exporter(Protocol.HTTP, server.getAddress().getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isTrue();
    }

    @Test
    @DisplayName("a request over maxRequestBytes is permanent, is not the endpoint's doing, and is never sent")
    void httpOversizeIsPermanentAndNotDownstream() throws Exception {
        var attempts = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            attempts.incrementAndGet();
            answer(exchange, 200, null, new byte[0], Map.of());
        });
        server.start();
        cleanup.add(() -> server.stop(0));
        var config = OtlpExporterConfig.builder(4318).host("127.0.0.1").port(server.getAddress().getPort())
                .retryPolicy(ONCE).maxRequestBytes(8).build();

        var rejected = rejected(send(exporter(Protocol.HTTP, config)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isFalse();
        assertThat(rejected.message()).contains("maxRequestBytes");
        assertThat(attempts).hasValue(0);
    }

    @Test
    @DisplayName("a 2xx response over maxResponseBytes is permanent: the data was accepted, a retry would duplicate it")
    void httpOversizeResponseIsPermanent() throws Exception {
        var server = http(200, "application/x-protobuf", new byte[256], Map.of());
        var config = OtlpExporterConfig.builder(4318).host("127.0.0.1").port(server.getAddress().getPort())
                .retryPolicy(ONCE).maxResponseBytes(16).build();

        var rejected = rejected(send(exporter(Protocol.HTTP, config)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isTrue();
    }

    @Test
    @DisplayName("a gRPC request over maxRequestBytes is permanent and never sent")
    void grpcOversizeRequestIsPermanent() throws Exception {
        var attempts = new AtomicInteger();
        var server = grpc(() -> {
            attempts.incrementAndGet();
            return Status.UNAVAILABLE.asRuntimeException();
        });
        var config = OtlpExporterConfig.builder(4317).host("127.0.0.1").port(server.getPort())
                .retryPolicy(ONCE).maxRequestBytes(8).build();

        var rejected = rejected(send(exporter(Protocol.GRPC, config)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(attempts).hasValue(0);
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    @DisplayName("a header supplier returning an invalid header is permanent and not downstream")
    void invalidSuppliedHeaderIsPermanent(Protocol protocol) throws Exception {
        var server = http(200);
        var config = OtlpExporterConfig.builder(4318).host("127.0.0.1").port(server.getAddress().getPort())
                .retryPolicy(ONCE).headers(() -> Map.of("bad header", "x")).build();

        var rejected = rejected(send(exporter(protocol, config)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isFalse();
    }

    // --- both protocols: transport failures and the deadline -----------------------------------

    @ParameterizedTest
    @EnumSource(Protocol.class)
    @DisplayName("a refused connection is a retryable downstream rejection")
    void connectionRefusedIsRetryable(Protocol protocol) throws Exception {
        var rejected = rejected(send(exporter(protocol, freePort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.downstream()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    @DisplayName("the total deadline elapsing is a retryable TIMEOUT attributed downstream")
    void totalDeadlineIsRetryableTimeout(Protocol protocol) {
        try (var server = ProgrammedOtlpServer.start(
                protocol, List.of(new ProgrammedResponse.Hang(Duration.ofSeconds(10))))) {
            var config = OtlpExporterConfig.builder(4317).host("127.0.0.1").port(server.port())
                    .timeout(Duration.ofMillis(300)).totalTimeout(Duration.ofMillis(300)).retryPolicy(ONCE).build();

            var rejected = rejected(send(exporter(protocol, config)));

            assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
            assertThat(rejected.reason()).isEqualTo(DeliveryReason.TIMEOUT);
            assertThat(rejected.downstream()).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    @DisplayName("an export after shutdown reports SHUTDOWN instead of throwing")
    void exportAfterShutdownIsShuttingDown(Protocol protocol) throws Exception {
        var server = http(200);
        var exporter = exporter(protocol, server.getAddress().getPort(), ONCE);
        exporter.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();

        var rejected = rejected(send(exporter));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.reason()).isEqualTo(DeliveryReason.SHUTDOWN);
    }

    // --- gRPC exporter -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("UNAVAILABLE with a positive pushback is retryable and carries the pushback as the hint")
    void grpcUnavailableWithPushback(boolean headersFirst) throws Exception {
        var server = grpc(() -> status(Status.UNAVAILABLE, 250), headersFirst);

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.downstream()).isTrue();
        assertThat(rejected.retryAfter()).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    @DisplayName("the pushback hint is dropped when the policy does not honour server hints")
    void grpcPushbackIgnored() throws Exception {
        var server = grpc(() -> status(Status.UNAVAILABLE, 250));

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE_IGNORING_HINTS)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.retryAfter()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("a negative pushback is the server telling the sender to stop: permanent")
    void grpcNegativePushbackIsPermanent(boolean headersFirst) throws Exception {
        var server = grpc(() -> status(Status.UNAVAILABLE, -1), headersFirst);

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isTrue();
    }

    @Test
    @DisplayName("a negative pushback stops the sender even when the policy ignores server hints")
    void grpcNegativePushbackIsPermanentEvenIgnoringHints() throws Exception {
        var server = grpc(() -> status(Status.UNAVAILABLE, -1));

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE_IGNORING_HINTS)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("RESOURCE_EXHAUSTED with a RetryInfo is retryable and carries the RetryInfo delay")
    void grpcResourceExhaustedWithRetryInfo(boolean headersFirst) throws Exception {
        var server = grpc(() -> withRetryInfo(Status.Code.RESOURCE_EXHAUSTED, Duration.ofMillis(1500)), headersFirst);

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.retryAfter()).isEqualTo(Duration.ofMillis(1500));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("RESOURCE_EXHAUSTED without a RetryInfo is permanent")
    void grpcResourceExhaustedWithoutRetryInfo(boolean headersFirst) throws Exception {
        var server = grpc(() -> Status.RESOURCE_EXHAUSTED.asRuntimeException(), headersFirst);

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(rejected.downstream()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"CANCELLED", "DEADLINE_EXCEEDED", "ABORTED", "OUT_OF_RANGE", "UNAVAILABLE", "DATA_LOSS"})
    @DisplayName("the gRPC codes the OTLP spec says to retry are retryable")
    void grpcRetryableCodes(String code) throws Exception {
        var server = grpc(() -> Status.fromCode(Status.Code.valueOf(code)).asRuntimeException());

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID_ARGUMENT", "INTERNAL", "UNKNOWN", "UNIMPLEMENTED", "PERMISSION_DENIED",
            "UNAUTHENTICATED", "NOT_FOUND"})
    @DisplayName("every other gRPC code is permanent")
    void grpcPermanentCodes(String code) throws Exception {
        var server = grpc(() -> Status.fromCode(Status.Code.valueOf(code)).asRuntimeException());

        var rejected = rejected(send(exporter(Protocol.GRPC, server.getPort(), ONCE)));

        assertThat(rejected.disposition()).isEqualTo(Disposition.PERMANENT);
    }

    // --- gateway: what the upstream sender is told ---------------------------------------------

    private GatewayRuntime gateway(Protocol protocol, OtlpExporterConfig downstream) {
        var builder = Gateway.builder();
        var receiverFactory = protocol == Protocol.GRPC ? OtlpReceiverFactory.grpc() : OtlpReceiverFactory.http();
        var exporterFactory = protocol == Protocol.GRPC ? OtlpExporterFactory.grpc() : OtlpExporterFactory.http();
        var receiver = builder.receiver(
                ComponentId.parse("otlp"), receiverFactory, receiverFactory.defaultConfig().withEphemeralPort());
        var exporter = builder.exporter(ComponentId.parse("out"), exporterFactory, downstream);
        builder.pipeline(PipelineId.of(SignalDescriptor.TRACES), p -> p.from(receiver).to(exporter));
        return started(builder);
    }

    private GatewayRuntime gatewayWith(Protocol protocol, ExporterFactory<ComponentConfig.None> exporterFactory) {
        var builder = Gateway.builder();
        var receiverFactory = protocol == Protocol.GRPC ? OtlpReceiverFactory.grpc() : OtlpReceiverFactory.http();
        var receiver = builder.receiver(
                ComponentId.parse("otlp"), receiverFactory, receiverFactory.defaultConfig().withEphemeralPort());
        var exporter = builder.exporter(ComponentId.parse("out"), exporterFactory);
        builder.pipeline(PipelineId.of(SignalDescriptor.TRACES), p -> p.from(receiver).to(exporter));
        return started(builder);
    }

    private GatewayRuntime started(GatewayBuilder builder) {
        var runtime = own(builder.build());
        runtime.start();
        return runtime;
    }

    /// A downstream for the gateway cases, which verify that a downstream failure reaches the upstream
    /// sender. A queued exporter acknowledges first and redelivers behind the sender, so these cases
    /// run the exporter unqueued, where the failure is relayed synchronously.
    private static OtlpExporterConfig downstream(int port, RetryPolicy retry) {
        return OtlpExporterConfig.builder(4317).host("127.0.0.1").port(port).retryPolicy(retry).noQueue().build();
    }

    private static int upstreamPort(GatewayRuntime runtime) {
        return runtime.boundPort(ComponentId.parse("otlp")).getAsInt();
    }

    private static HttpResponse<byte[]> postHttp(int port) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/traces"))
                .header("content-type", "application/x-protobuf")
                .POST(HttpRequest.BodyPublishers.ofByteArray(request().toByteArray()))
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private StatusRuntimeException callGrpc(int port) {
        ManagedChannel channel = own(new ChannelCloser(
                ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build())).channel;
        try {
            TraceServiceGrpc.newBlockingStub(channel).export(request());
        } catch (StatusRuntimeException e) {
            return e;
        }
        throw new AssertionError("expected the gateway to answer with a gRPC error");
    }

    private record ChannelCloser(ManagedChannel channel) implements AutoCloseable {
        @Override
        public void close() {
            channel.shutdownNow();
        }
    }

    @Test
    @DisplayName("HTTP gateway: a downstream that always answers 503 is a 503 upstream, so the sender retries")
    void httpGatewayRelaysRetryableStatus() throws Exception {
        var server = http(503);
        var runtime = gateway(Protocol.HTTP, downstream(server.getAddress().getPort(), FAST));

        var response = postHttp(upstreamPort(runtime));

        assertThat(response.statusCode()).isEqualTo(503);
    }

    @Test
    @DisplayName("HTTP gateway: a downstream 503 Retry-After reaches the upstream sender")
    void httpGatewayRelaysRetryAfter() throws Exception {
        var server = http(503, null, new byte[0], Map.of("Retry-After", "7"));
        var runtime = gateway(Protocol.HTTP, downstream(server.getAddress().getPort(), ONCE));

        var response = postHttp(upstreamPort(runtime));

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.headers().firstValue("Retry-After")).contains("7");
    }

    @Test
    @DisplayName("HTTP gateway: a refused downstream connection is a 503 upstream")
    void httpGatewayRelaysConnectionRefused() throws Exception {
        var runtime = gateway(Protocol.HTTP, downstream(freePort(), FAST));

        assertThat(postHttp(upstreamPort(runtime)).statusCode()).isEqualTo(503);
    }

    @Test
    @DisplayName("HTTP gateway: a downstream 400 is permanent upstream, so the sender does not retry")
    void httpGatewayRelaysPermanentStatus() throws Exception {
        var server = http(400);
        var runtime = gateway(Protocol.HTTP, downstream(server.getAddress().getPort(), FAST));

        assertThat(postHttp(upstreamPort(runtime)).statusCode()).isEqualTo(500);
    }

    @Test
    @DisplayName("gRPC gateway: a downstream UNAVAILABLE with pushback is UNAVAILABLE with that pushback upstream")
    void grpcGatewayRelaysUnavailableAndPushback() throws Exception {
        var server = grpc(() -> status(Status.UNAVAILABLE, 250));
        var runtime = gateway(Protocol.GRPC, downstream(server.getPort(), ONCE));

        var error = callGrpc(upstreamPort(runtime));

        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(error.getTrailers().get(PUSHBACK)).isEqualTo("250");
    }

    @Test
    @DisplayName("gRPC gateway: a refused downstream connection is UNAVAILABLE upstream")
    void grpcGatewayRelaysConnectionRefused() throws Exception {
        var runtime = gateway(Protocol.GRPC, downstream(freePort(), FAST));

        assertThat(callGrpc(upstreamPort(runtime)).getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    @DisplayName("gRPC gateway: a downstream permanent rejection is INTERNAL upstream")
    void grpcGatewayRelaysPermanentStatus() throws Exception {
        var server = grpc(() -> Status.INVALID_ARGUMENT.asRuntimeException());
        var runtime = gateway(Protocol.GRPC, downstream(server.getPort(), FAST));

        assertThat(callGrpc(upstreamPort(runtime)).getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @Test
    @DisplayName("gRPC gateway: a downstream stop-retry pushback is INTERNAL upstream")
    void grpcGatewayRelaysNegativePushback() throws Exception {
        var server = grpc(() -> status(Status.UNAVAILABLE, -1));
        var runtime = gateway(Protocol.GRPC, downstream(server.getPort(), FAST));

        assertThat(callGrpc(upstreamPort(runtime)).getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    @DisplayName("a custom exporter that throws is a retryable failure upstream")
    void throwingExporterIsRetryableUpstream(Protocol protocol) throws Exception {
        ExporterFactory<ComponentConfig.None> thrower = ExporterFactory.of("thrower", (config, deps, provenance) -> {
            throw new IllegalStateException("boom");
        });
        var runtime = gatewayWith(protocol, thrower);

        if (protocol == Protocol.HTTP) {
            assertThat(postHttp(upstreamPort(runtime)).statusCode()).isEqualTo(503);
        } else {
            assertThat(callGrpc(upstreamPort(runtime)).getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        }
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    @DisplayName("a custom exporter whose stage fails is a retryable failure upstream")
    void failingStageIsRetryableUpstream(Protocol protocol) throws Exception {
        ExporterFactory<ComponentConfig.None> failing = ExporterFactory.of("failing", (config, deps, provenance) ->
                CompletableFuture.failedFuture(new java.io.IOException("disk full")));
        var runtime = gatewayWith(protocol, failing);

        if (protocol == Protocol.HTTP) {
            assertThat(postHttp(upstreamPort(runtime)).statusCode()).isEqualTo(503);
        } else {
            assertThat(callGrpc(upstreamPort(runtime)).getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        }
    }
}
