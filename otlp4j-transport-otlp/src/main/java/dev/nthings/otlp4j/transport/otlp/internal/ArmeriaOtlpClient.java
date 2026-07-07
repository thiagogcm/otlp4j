package dev.nthings.otlp4j.transport.otlp.internal;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.ClientRequestContextCaptor;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.grpc.GrpcClients;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.logging.RequestLog;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.transport.common.ClientHeaders;
import dev.nthings.otlp4j.transport.common.ExporterMetrics;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The consolidated OTLP implementation of the [OtlpClient] SPI, built on Armeria's gRPC and HTTP
/// clients. One instance speaks one protocol (selected at construction): OTLP/gRPC through per-signal
/// blocking stubs, OTLP/HTTP through a [WebClient] posting binary protobuf to the per-signal
/// `/v1/...` paths. It honours the full [ClientConfig]: TLS credentials, per-call headers, gzip, and
/// the per-attempt and total deadlines.
///
/// Retry, backoff, and the server throttling hints are delegated to Armeria's [RetryingClient] through
/// the [ArmeriaRetryRules] decorator installed on each protocol client: the per-attempt deadline is the
/// retrying client's per-attempt response timeout, the total deadline is the client-level response
/// timeout, and the folding of `Retry-After`, gRPC pushback (including the negative stop-retry
/// directive), and `RetryInfo` follows the documented precedence. Each rule-driven re-send is counted
/// into the exporter's `otlp4j.exporter.export.retries` instrument by reading the retrying context's
/// per-attempt children after the delivery settles, so the count is exactly one per retry.
///
/// **Internal.** Part of the transport layer; obtained via the SPI.
public final class ArmeriaOtlpClient implements OtlpClient {

    private static final Logger log = LoggerFactory.getLogger(ArmeriaOtlpClient.class);

    /// Runs each blocking teardown wait on a virtual thread, greppable by thread name if it stalls.
    private static final Executor SHUTDOWN_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-armeria-client-shutdown").start(task);

    private final ClientConfig config;
    private final ExporterMetrics metrics;
    private final boolean compress;
    private final ExecutorService executor;
    private final @Nullable ClientFactory clientFactory;
    private final Map<SignalDescriptor<?>, Export> exports = new HashMap<>();

    /// Standalone and lifecycle-test constructor: no exporter metrics.
    ///
    /// @param config   the transport config
    /// @param protocol the wire protocol to speak
    public ArmeriaOtlpClient(ClientConfig config, Protocol protocol) {
        this(config, ExporterMetrics.noop(), protocol);
    }

    /// Full constructor.
    ///
    /// @param config   the transport config
    /// @param metrics  the exporter's instrument owner (used for retry accounting in a later lot)
    /// @param protocol the wire protocol to speak
    public ArmeriaOtlpClient(ClientConfig config, ExporterMetrics metrics, Protocol protocol) {
        this.config = config;
        this.metrics = metrics;
        this.compress = config.compression() == Compression.GZIP;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.clientFactory = ArmeriaTls.clientFactory(config.tls());
        var base = ArmeriaTls.clientScheme(config.tls()) + "://" + authority(config.host(), config.port());
        switch (protocol) {
            case GRPC -> buildGrpcExports(base);
            case HTTP -> buildHttpExports(base);
        }
        log.debug("created consolidated OTLP {} client for {}", protocol, base);
    }

    private void buildGrpcExports(String base) {
        // The total deadline is the client-level response timeout across all attempts; the per-attempt
        // deadline and the retry/backoff/hint folding are the retrying decorator's job.
        var builder = GrpcClients.builder(base + "/")
                .responseTimeout(config.totalTimeout())
                .decorator(ArmeriaRetryRules.decorator(Protocol.GRPC, config.retry(), config.timeout()));
        if (clientFactory != null) {
            builder.factory(clientFactory);
        }
        var traceStub = builder.build(TraceServiceGrpc.TraceServiceFutureStub.class);
        var metricsStub = builder.build(MetricsServiceGrpc.MetricsServiceFutureStub.class);
        var logsStub = builder.build(LogsServiceGrpc.LogsServiceFutureStub.class);
        var profilesStub = builder.build(ProfilesServiceGrpc.ProfilesServiceFutureStub.class);
        exports.put(SignalDescriptor.TRACES,
                grpcExport(SignalCodec.TRACES, r -> call(traceStub).export(r)));
        exports.put(SignalDescriptor.METRICS,
                grpcExport(SignalCodec.METRICS, r -> call(metricsStub).export(r)));
        exports.put(SignalDescriptor.LOGS,
                grpcExport(SignalCodec.LOGS, r -> call(logsStub).export(r)));
        exports.put(SignalDescriptor.PROFILES,
                grpcExport(SignalCodec.PROFILES, r -> call(profilesStub).export(r)));
    }

    private void buildHttpExports(String base) {
        var builder = WebClient.builder(base)
                .responseTimeout(config.totalTimeout())
                .decorator(ArmeriaRetryRules.decorator(Protocol.HTTP, config.retry(), config.timeout()));
        if (clientFactory != null) {
            builder.factory(clientFactory);
        }
        var webClient = builder.build();
        exports.put(SignalDescriptor.TRACES,
                httpExport(webClient, SignalCodec.TRACES, config.path() + OtlpHttpPaths.TRACES_PATH));
        exports.put(SignalDescriptor.METRICS,
                httpExport(webClient, SignalCodec.METRICS, config.path() + OtlpHttpPaths.METRICS_PATH));
        exports.put(SignalDescriptor.LOGS,
                httpExport(webClient, SignalCodec.LOGS, config.path() + OtlpHttpPaths.LOGS_PATH));
        exports.put(SignalDescriptor.PROFILES,
                httpExport(webClient, SignalCodec.PROFILES, config.path() + OtlpHttpPaths.PROFILES_PATH));
    }

    @Override
    public CompletionStage<DeliveryResult> export(TelemetryPayload payload, Provenance provenance) {
        var export = exports.get(payload.signal());
        // Initiate the delivery on the caller's thread and hand back Armeria's own future: the request
        // is enqueued to the client event loop and the completion is delivered there, with no
        // per-export carrier thread. The whole delivery is bounded by the client-level response timeout
        // (config.totalTimeout()); each in-flight attempt is bounded by the retrying decorator's
        // per-attempt response timeout. A failure preparing the request surfaces as a failed stage, so
        // export() honours the SPI contract of never throwing synchronously.
        try {
            return export.result(payload, provenance);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /// Binds a [SignalCodec] to a gRPC unary RPC: reads the payload's proto request view, invokes the
    /// async RPC (retried by the installed decorator), and interprets the response as a [DeliveryResult].
    private <T, REQ extends MessageLite, RESP extends MessageLite> Export grpcExport(
            SignalCodec<T, REQ, RESP> codec, Function<REQ, ListenableFuture<RESP>> rpc) {
        return (payload, provenance) -> {
            @SuppressWarnings("unchecked")
            var request = (REQ) payload.view(PayloadView.PROTO_REQUEST);
            return withRetryAccounting(() ->
                    toCompletableFuture(rpc.apply(request))
                            .thenApply(response -> codec.deliveryResult(response, provenance)));
        };
    }

    /// Binds a [SignalCodec] to an OTLP/HTTP POST at `path`: serialises the payload, posts it (retried
    /// by the installed decorator), and maps a 2xx response to a [DeliveryResult]; any other status
    /// throws.
    private <T, REQ extends MessageLite, RESP extends MessageLite> Export httpExport(
            WebClient webClient, SignalCodec<T, REQ, RESP> codec, String path) {
        return (payload, provenance) -> {
            var body = serialize(payload);
            return withRetryAccounting(() ->
                    webClient.execute(request(path, body)).aggregate().thenApply(response -> {
                        var code = response.status().code();
                        if (code >= 200 && code < 300) {
                            return codec.deliveryResult(
                                    parseResponse(codec, response.content().array()), provenance);
                        }
                        throw rejection(path, response);
                    }));
        };
    }

    /// Initiates `call` under a captured client context and returns a stage that settles once the
    /// delivery has settled and its retries have been counted. The request is initiated synchronously
    /// (the async stub / web client returns a future immediately), so the captor observes its context
    /// before it is closed. The returned stage completes only after the retry instrument has been
    /// written, so a caller that observes completion also observes the retry count. A total-deadline
    /// response timeout is mapped onto a [TimeoutException] so the caller boundary reads a timeout
    /// rather than a transport rejection; the per-attempt response timeout never surfaces here, since
    /// the retrying client consumes it as a failed attempt and retries.
    ///
    /// The returned stage carries a total-budget `orTimeout`: gating completion on the retry-log
    /// settling means the caller-observed completion can otherwise lag the wire response timeout while
    /// a timed-out request against an unresponsive peer finishes settling its log, so this bounds the
    /// caller's wait on the total budget independently of that settling. It is a scheduled timer, not a
    /// carrier thread, so it keeps the per-export thread hop removed.
    private CompletionStage<DeliveryResult> withRetryAccounting(
            Supplier<CompletableFuture<DeliveryResult>> call) {
        ClientRequestContext ctx;
        CompletableFuture<DeliveryResult> stage;
        try (ClientRequestContextCaptor captor = Clients.newContextCaptor()) {
            stage = call.get();
            ctx = captor.getOrNull();
        }
        var settled = new CompletableFuture<DeliveryResult>();
        stage.whenComplete((result, error) ->
                recordRetries(ctx).whenComplete((ignored, recordFailure) -> {
                    if (error == null) {
                        settled.complete(result);
                    } else {
                        settled.completeExceptionally(mapTotalTimeout(asRuntime(error)));
                    }
                }));
        return settled.orTimeout(config.totalTimeout().toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Counts the retrying decorator's re-sends into the exporter's retry instrument once the request
    /// log completes, off the export thread. A no-op instrument owner (the standalone and lifecycle
    /// paths) or an uncaptured context skips the log read entirely and completes at once.
    private CompletionStage<Void> recordRetries(@Nullable ClientRequestContext ctx) {
        if (ctx == null || metrics == ExporterMetrics.noop()) {
            return CompletableFuture.completedFuture(null);
        }
        return ctx.log().whenComplete().thenAccept(this::recordRetriesFromLog);
    }

    /// Adds one retry to the exporter's retry instrument per rule-driven re-send, read from the number
    /// of per-attempt children the retrying context accumulated (total attempts minus the first).
    private void recordRetriesFromLog(RequestLog requestLog) {
        var attempts = requestLog.children().isEmpty() ? 1 : requestLog.children().size();
        for (int i = 1; i < attempts; i++) {
            metrics.recordRetry();
        }
    }

    /// Narrows a settled stage's failure to a [RuntimeException] for [#mapTotalTimeout], wrapping a
    /// non-runtime throwable in a [CompletionException] so its cause chain stays walkable.
    private static RuntimeException asRuntime(Throwable error) {
        return error instanceof RuntimeException runtime ? runtime : new CompletionException(error);
    }

    /// Bridges a gRPC [ListenableFuture] onto a [CompletableFuture], completing inline on the thread
    /// that settles the future (Armeria's event loop) and unwrapping the [ExecutionException] so the
    /// stage carries the same `StatusRuntimeException` the blocking stub would have thrown.
    private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> future) {
        var result = new CompletableFuture<T>();
        future.addListener(() -> {
            try {
                result.complete(future.get());
            } catch (ExecutionException e) {
                result.completeExceptionally(e.getCause() != null ? e.getCause() : e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result.completeExceptionally(e);
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }, Runnable::run);
        return result;
    }

    /// Rewraps a total-deadline response timeout as a [TimeoutException] (inside a [CompletionException]
    /// so the exporter boundary unwraps it to a timeout), and passes every other failure through
    /// unchanged.
    private static RuntimeException mapTotalTimeout(RuntimeException error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof ResponseTimeoutException) {
                return new CompletionException(
                        new TimeoutException("OTLP export exceeded the total deadline"));
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return error;
    }

    /// Applies optional gzip compression and the resolved per-call headers to a gRPC stub. The
    /// per-attempt and total deadlines are the retrying decorator's per-attempt response timeout and the
    /// client-level response timeout, so no per-call gRPC deadline is set here (one would span every
    /// retry and abort the whole delivery at the first per-attempt budget).
    private <S extends AbstractStub<S>> S call(S stub) {
        var s = stub;
        if (compress) {
            s = s.withCompression("gzip");
        }
        var headers = ClientHeaders.resolve(config.headers(), config.headerSupplier());
        if (!headers.isEmpty()) {
            s = s.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(toMetadata(headers)));
        }
        return s;
    }

    private HttpRequest request(String path, byte[] body) {
        var builder = HttpRequest.builder().post(path).content(MediaType.X_PROTOBUF, body);
        if (compress) {
            builder.header(HttpHeaderNames.CONTENT_ENCODING, "gzip");
        }
        ClientHeaders.resolve(config.headers(), config.headerSupplier()).forEach(builder::header);
        return builder.build();
    }

    private byte[] serialize(TelemetryPayload payload) {
        var size = Math.toIntExact(payload.estimatedSerializedSize());
        var out = new ByteArrayOutputStream(size);
        try {
            if (compress) {
                try (var gz = new GZIPOutputStream(out)) {
                    payload.writeTo(gz);
                }
            } else {
                payload.writeTo(out);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize OTLP/HTTP request body", e);
        }
        return out.toByteArray();
    }

    private static <RESP extends MessageLite> RESP parseResponse(
            SignalCodec<?, ?, RESP> codec, byte[] body) {
        try {
            return codec.parseResponse(body);
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException("malformed OTLP/HTTP response body", e);
        }
    }

    private static RuntimeException rejection(String path, AggregatedHttpResponse response) {
        var code = response.status().code();
        var text = response.contentUtf8().strip();
        return new IllegalStateException(
                "OTLP/HTTP export to " + path + " rejected: HTTP " + code + (text.isEmpty() ? "" : " - " + text));
    }

    private static Metadata toMetadata(Map<String, String> headers) {
        var md = new Metadata();
        headers.forEach((name, value) ->
                md.put(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER), value));
        return md;
    }

    /// Wraps an IPv6 literal in brackets to form a valid URL authority; other hosts pass through.
    private static String authority(String host, int port) {
        var h = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        return h + ":" + port;
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        var deadline = System.nanoTime() + timeout.toNanos();
        return CompletableFuture.runAsync(() -> {
            drainExecutor(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            closeFactoryWithin(Math.max(0, deadline - System.nanoTime()));
        }, SHUTDOWN_EXECUTOR);
    }

    /// Closes the [ClientFactory] within the remaining `budgetNanos`, so the returned shutdown stage
    /// honours the caller's deadline even when a slow or unreachable collector stalls connection
    /// teardown. Armeria's own `close()` is unbounded and not usefully interruptible, so this bounds
    /// the wait on the async close and abandons it on timeout (the orphaned close finishes in Armeria's
    /// own threads) rather than blocking the shutdown stage past the deadline.
    private void closeFactoryWithin(long budgetNanos) {
        if (clientFactory == null) {
            return;
        }
        try {
            clientFactory.closeAsync().get(budgetNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            log.warn("client factory did not close within the shutdown deadline; abandoning the close");
        } catch (ExecutionException e) {
            log.warn("client factory close failed", e.getCause() != null ? e.getCause() : e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /// Drains the per-export executor within `budget`, interrupting outstanding exports on timeout.
    private void drainExecutor(Duration budget) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(budget.toNanos(), TimeUnit.NANOSECONDS)) {
                log.warn("per-export executor did not terminate within {}; interrupting outstanding exports",
                        budget);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /// One signal's export: reads the payload's wire representation, sends it asynchronously, and maps
    /// the response to a [DeliveryResult].
    @FunctionalInterface
    private interface Export {
        CompletionStage<DeliveryResult> result(TelemetryPayload payload, Provenance provenance);
    }
}
