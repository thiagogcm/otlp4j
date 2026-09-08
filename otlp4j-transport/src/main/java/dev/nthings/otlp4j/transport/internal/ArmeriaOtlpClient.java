package dev.nthings.otlp4j.transport.internal;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.rpc.Status;
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
import dev.nthings.otlp4j.codec.PayloadWire;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.Compression;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.transport.internal.ClientHeaders;
import dev.nthings.otlp4j.transport.internal.ExporterMetrics;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The consolidated OTLP implementation of a wire client, built on Armeria's gRPC and HTTP
/// clients. One instance speaks one protocol (selected at construction): OTLP/gRPC through per-signal
/// asynchronous stubs, OTLP/HTTP through a [WebClient] posting binary protobuf to the per-signal
/// `/v1/...` paths. It honours the full [OtlpExporterConfig]: TLS credentials, per-call headers, gzip, and
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
/// **Internal.** Part of the transport layer; internal.
public final class ArmeriaOtlpClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ArmeriaOtlpClient.class);

    /// Runs each blocking teardown wait on a virtual thread, greppable by thread name if it stalls.
    private static final Executor SHUTDOWN_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-armeria-client-shutdown").start(task);

    private final OtlpExporterConfig config;
    private final ExporterMetrics metrics;
    private final Protocol protocol;
    private final boolean compress;
    private @Nullable ClientFactory clientFactory;
    private final Map<SignalDescriptor<?>, Export> exports = new HashMap<>();
    private final Set<CompletableFuture<DeliveryResult>> inFlight = ConcurrentHashMap.newKeySet();
    private volatile boolean started;
    private boolean closing;
    private @Nullable CompletionStage<Void> shutdownStage;

    /// Standalone and lifecycle-test constructor: no exporter metrics.
    ///
    /// @param config   the transport config
    /// @param protocol the wire protocol to speak
    public ArmeriaOtlpClient(OtlpExporterConfig config, Protocol protocol) {
        this(config, ExporterMetrics.noop(), protocol);
    }

    /// Full constructor.
    ///
    /// @param config   the transport config
    /// @param metrics  the exporter's instrument owner
    /// @param protocol the wire protocol to speak
    public ArmeriaOtlpClient(OtlpExporterConfig config, ExporterMetrics metrics, Protocol protocol) {
        this.config = config;
        this.metrics = metrics;
        this.protocol = protocol;
        this.compress = config.compression() == Compression.GZIP;
        this.clientFactory = null;
    }

    /** Creates the protocol client resources. This method is idempotent and does no network I/O. */
    public synchronized void start() {
        if (closing) {
            throw new IllegalStateException("client is already stopping");
        }
        if (started) {
            return;
        }
        try {
            this.clientFactory = ArmeriaTls.clientFactory(config.tls());
            var base = ArmeriaTls.clientScheme(config.tls()) + "://" + authority(config.host(), config.port());
            switch (protocol) {
                case GRPC -> buildGrpcExports(base);
                case HTTP -> buildHttpExports(base);
            }
            started = true;
            log.debug("created consolidated OTLP {} client for {}", protocol, base);
        } catch (RuntimeException | Error failure) {
            // ClientFactory is allocated before protocol-specific stubs are built. If a limit,
            // decorator, or TLS validation fails during construction, release every resource acquired
            // by this attempt so a failed start is retry-safe and does not leak event-loop threads.
            exports.clear();
            var factory = clientFactory;
            clientFactory = null;
            if (factory != null) {
                try {
                    factory.closeAsync().get(5, TimeUnit.SECONDS);
                } catch (InterruptedException cleanupFailure) {
                    Thread.currentThread().interrupt();
                    failure.addSuppressed(cleanupFailure);
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    private void buildGrpcExports(String base) {
        // The total deadline is the client-level response timeout across all attempts; the per-attempt
        // deadline and the retry/backoff/hint folding are the retrying decorator's job.
        var builder = GrpcClients.builder(base + "/")
                .responseTimeout(config.timeout().total())
                .maxRequestMessageLength(Math.toIntExact(config.maxRequestBytes()))
                .maxResponseMessageLength(Math.toIntExact(config.maxResponseBytes()))
                .decorator(ArmeriaRetryRules.decorator(Protocol.GRPC, config.retry(), config.timeout().perAttempt()));
        if (clientFactory != null) {
            builder.factory(clientFactory);
        }
        var traceStub = prepareStub(builder.build(TraceServiceGrpc.TraceServiceFutureStub.class));
        var metricsStub = prepareStub(builder.build(MetricsServiceGrpc.MetricsServiceFutureStub.class));
        var logsStub = prepareStub(builder.build(LogsServiceGrpc.LogsServiceFutureStub.class));
        var profilesStub = prepareStub(builder.build(ProfilesServiceGrpc.ProfilesServiceFutureStub.class));
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
                .responseTimeout(config.timeout().total())
                .maxResponseLength(config.maxResponseBytes())
                .decorator(ArmeriaRetryRules.decorator(Protocol.HTTP, config.retry(), config.timeout().perAttempt()));
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

    /// Exports one `payload`, mapping the endpoint's response to a [DeliveryResult] attributed to
    /// `provenance`. Never throws synchronously: a failure preparing the request surfaces as a failed
    /// stage.
    ///
    /// @param payload    the payload to export
    /// @param provenance the provenance to attribute the result to
    /// @return a stage completing with the export outcome
    public <T> CompletionStage<DeliveryResult> export(Payload<T> payload, Provenance provenance) {
        synchronized (this) {
            if (!started || closing) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException(closing
                                ? "exporter is stopping"
                                : "exporter has not been started"));
            }
            var export = exports.get(payload.signal());
            // Admission and registration are one critical section. Shutdown takes the same lock
            // before it snapshots in-flight work, so a request cannot slip between its snapshot and
            // registration and outlive the returned shutdown stage.
            try {
                var delivery = export.result(payload, provenance).toCompletableFuture();
                inFlight.add(delivery);
                delivery.whenComplete((result, error) -> inFlight.remove(delivery));
                return delivery;
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    /// Binds a [SignalCodec] to a gRPC unary RPC: reads the payload's proto request view, invokes the
    /// async RPC (retried by the installed decorator), and interprets the response as a [DeliveryResult].
    private <T, REQ extends MessageLite, RESP extends MessageLite> Export grpcExport(
            SignalCodec<T, REQ, RESP> codec, Function<REQ, ListenableFuture<RESP>> rpc) {
        return (payload, provenance) -> {
            @SuppressWarnings("unchecked")
            var request = PayloadWire.request(codec, (Payload<T>) payload);
            return withRetryAccounting(cancel -> {
                var response = toCompletableFuture(rpc.apply(request));
                cancel.set(() -> response.cancel(true));
                var mapped = response.thenApply(value -> codec.deliveryResult(value, provenance));
                return mapped;
            });
        };
    }

    /// Binds a [SignalCodec] to an OTLP/HTTP POST at `path`: serialises the payload, posts it (retried
    /// by the installed decorator), and maps a 2xx response to a [DeliveryResult]; any other status
    /// throws.
    private <T, REQ extends MessageLite, RESP extends MessageLite> Export httpExport(
            WebClient webClient, SignalCodec<T, REQ, RESP> codec, String path) {
        return (payload, provenance) -> {
            var body = serialize(payload);
            return withRetryAccounting(cancel -> {
                var response = webClient.execute(request(path, body));
                // Abort the public response stream as well as the captured request context. Armeria
                // wrappers may delegate one operation but not the other depending on the installed
                // retry decorator, so retaining both paths keeps cancellation effective for HTTP/1
                // and HTTP/2.
                cancel.set(response::abort);
                var aggregate = response.aggregate();
                var mapped = aggregate.thenApply(result -> {
                    var code = result.status().code();
                    if (code >= 200 && code < 300) {
                        return codec.deliveryResult(
                                parseResponse(codec, result.content().array()), provenance);
                    }
                    throw rejection(path, result);
                });
                return mapped;
            });
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
            Function<WireCancellation, CompletableFuture<DeliveryResult>> call) {
        var cancellation = new WireCancellation();
        List<ClientRequestContext> contexts;
        CompletableFuture<DeliveryResult> stage;
        try (ClientRequestContextCaptor captor = Clients.newContextCaptor()) {
            stage = call.apply(cancellation);
            contexts = captor.getAll();
        }
        var context = contexts.isEmpty() ? null : contexts.getLast();
        var settled = new CompletableFuture<DeliveryResult>();
        stage.whenComplete((result, error) ->
                (metrics == ExporterMetrics.noop() ? CompletableFuture.completedFuture(null) : recordRetries(context))
                        .whenComplete((ignored, recordFailure) -> {
                    if (error == null) {
                        settled.complete(result);
                    } else {
                        settled.completeExceptionally(mapTotalTimeout(asRuntime(error)));
                    }
                }));
        settled.whenComplete((result, error) -> {
            if (settled.isCancelled()) {
                cancelWire(stage, contexts, cancellation);
            }
        });
        var bounded = settled.orTimeout(timeoutNanos(config.timeout().total()), TimeUnit.NANOSECONDS);
        bounded.whenComplete((result, error) -> {
            // CompletableFuture.orTimeout only settles the dependent stage. Cancel the active
            // Armeria request as well when the total budget expires, otherwise a timed-out export
            // would remain in-flight until the remote peer eventually responds.
            if (error != null && !stage.isDone()) {
                cancelWire(stage, contexts, cancellation);
            }
        });
        return bounded;
    }

    private static void cancelWire(
            CompletableFuture<?> stage,
            List<ClientRequestContext> contexts,
            WireCancellation cancellation) {
        cancellation.cancel();
        stage.cancel(true);
        // RetryingClient may create a child context after the outer context was captured. Cancelling
        // every context already visible to the captor reaches both the current attempt and its
        // decorator, while the stage/action above handles protocol-specific future cancellation.
        contexts.forEach(context -> {
            context.log().children().forEach(child -> child.context().cancel());
            context.cancel();
        });
    }

    /**
     * Cancellation of a mapped response future does not cancel the wire operation by itself. Keep the
     * operation-specific abort action beside the logical delivery and run it from every cancellation
     * path, including a deadline that expires before Armeria settles the response.
     */
    private static final class WireCancellation {
        private final AtomicReference<Runnable> action = new AtomicReference<>();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        void set(Runnable action) {
            Objects.requireNonNull(action, "action");
            this.action.set(action);
            if (cancelled.get()) {
                runOnce();
            }
        }

        void cancel() {
            cancelled.set(true);
            runOnce();
        }

        private void runOnce() {
            var action = this.action.getAndSet(null);
            if (action != null) {
                action.run();
            }
        }
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
        var result = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                future.cancel(mayInterruptIfRunning);
                return super.cancel(mayInterruptIfRunning);
            }
        };
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

    /// Decorates a gRPC stub once at construction with the settings that never vary per export: optional
    /// gzip compression (the compression mode is fixed for the client's lifetime) and, when no per-call
    /// header supplier is set, the fixed request headers baked in as an attach-headers interceptor. When
    /// a header supplier is set the headers must be re-resolved per export, so they are attached in
    /// [#call] instead and left off here.
    ///
    /// The per-attempt and total deadlines are the retrying decorator's per-attempt response timeout and
    /// the client-level response timeout, so no per-call gRPC deadline is set (one would span every retry
    /// and abort the whole delivery at the first per-attempt budget).
    private <S extends AbstractStub<S>> S prepareStub(S stub) {
        var s = compress ? stub.withCompression("gzip") : stub;
        if (config.headerSupplier() == null) {
            var headers = ClientHeaders.resolve(config.headers(), null);
            if (!headers.isEmpty()) {
                s = s.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(toMetadata(headers)));
            }
        }
        return s;
    }

    /// Applies the per-export header decoration to a stub already prepared by [#prepareStub]. With no
    /// header supplier the stub was fully decorated at construction, so it is returned unchanged; only a
    /// supplier-backed configuration re-resolves the headers on each export and attaches them here.
    private <S extends AbstractStub<S>> S call(S stub) {
        if (config.headerSupplier() == null) {
            return stub;
        }
        var headers = ClientHeaders.resolve(config.headers(), config.headerSupplier());
        return headers.isEmpty()
                ? stub
                : stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(toMetadata(headers)));
    }

    private HttpRequest request(String path, byte[] body) {
        var builder = HttpRequest.builder().post(path).content(MediaType.X_PROTOBUF, body);
        if (compress) {
            builder.header(HttpHeaderNames.CONTENT_ENCODING, "gzip");
        }
        ClientHeaders.resolve(config.headers(), config.headerSupplier()).forEach(builder::header);
        return builder.build();
    }

    private byte[] serialize(Payload<?> payload) {
        var codec = SignalCodec.forDescriptor(payload.signal());
        var request = request(codec, payload);
        var size = request.getSerializedSize();
        if (size > config.maxRequestBytes()) {
            throw new IllegalArgumentException(
                    "OTLP request exceeds maxRequestBytes=" + config.maxRequestBytes());
        }
        var out = new ByteArrayOutputStream(size);
        try {
            if (compress) {
                try (var gz = new GZIPOutputStream(out)) {
                    request.writeTo(gz);
                }
            } else {
                request.writeTo(out);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize OTLP/HTTP request body", e);
        }
        var bytes = out.toByteArray();
        if (bytes.length > config.maxRequestBytes()) {
            throw new IllegalArgumentException(
                    "OTLP request exceeds maxRequestBytes=" + config.maxRequestBytes());
        }
        return bytes;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <REQ extends MessageLite> REQ request(
            SignalCodec<?, REQ, ?> codec, Payload<?> payload) {
        return (REQ) PayloadWire.request((SignalCodec) codec, (Payload) payload);
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
        if (!response.content().isEmpty()) {
            try {
                var status = Status.parseFrom(response.content().array());
                if (!status.getMessage().isBlank()) {
                    text = status.getMessage();
                }
            } catch (InvalidProtocolBufferException ignored) {
                // A non-OTLP peer may still return plain text. Preserve that useful diagnostic.
            }
        }
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

    /// Waits for exports accepted before the flush call; the client does not maintain a buffer.
    ///
    /// @param timeout the flush deadline
    /// @return a completed stage
    public CompletionStage<FlushReport> flush(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        final List<CompletableFuture<DeliveryResult>> snapshot;
        synchronized (this) {
            snapshot = List.copyOf(inFlight);
        }
        if (snapshot.isEmpty()) {
            return CompletableFuture.completedFuture(FlushReport.empty());
        }
        var all = CompletableFuture.allOf(snapshot.toArray(CompletableFuture[]::new));
        // DeliveryResult is already the authoritative outcome for this unbuffered client. Flush only
        // establishes a completion barrier and must not turn a direct rejected/partial result into a
        // second loss report.
        return all.handle((ignored, failure) -> FlushReport.empty())
                .orTimeout(timeoutNanos(timeout), TimeUnit.NANOSECONDS);
    }

    private static long timeoutNanos(Duration timeout) {
        return TimeUnit.NANOSECONDS.convert(timeout);
    }

    private static long remainingNanos(long startedAt, long budget) {
        return Math.max(0, budget - (System.nanoTime() - startedAt));
    }

    /// Drains the client with a ten-second grace and exposes teardown failures to the caller.
    @Override
    public void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }

    /// Drains in-flight exports and releases the client's transport resources within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes when teardown finishes
    public synchronized CompletionStage<Void> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        if (shutdownStage != null) {
            return shutdownStage;
        }
        if (!started) {
            closing = true;
            shutdownStage = CompletableFuture.completedFuture(null);
            return shutdownStage;
        }
        closing = true;
        started = false;
        var startedAt = System.nanoTime();
        var budget = timeoutNanos(timeout);
        shutdownStage = CompletableFuture.runAsync(() -> {
            Throwable firstFailure = awaitInFlight(Duration.ofNanos(remainingNanos(startedAt, budget)));
            var failure = closeFactoryWithin(remainingNanos(startedAt, budget));
            if (firstFailure == null) {
                firstFailure = failure;
            } else if (failure != null && failure != firstFailure) {
                firstFailure.addSuppressed(failure);
            }
            if (firstFailure != null) {
                throw new CompletionException(firstFailure);
            }
        }, SHUTDOWN_EXECUTOR);
        return shutdownStage;
    }

    /// Waits for exports already accepted by this client before closing the shared factory. A
    /// factory close alone may release the connection while a caller-owned export stage is still
    /// pending, which makes shutdown report success before the last delivery has settled.
    private @Nullable Throwable awaitInFlight(Duration budget) {
        final List<CompletableFuture<DeliveryResult>> snapshot;
        synchronized (this) {
            snapshot = List.copyOf(inFlight);
        }
        if (snapshot.isEmpty()) {
            return null;
        }
        try {
            CompletableFuture.allOf(snapshot.toArray(CompletableFuture[]::new))
                    .get(timeoutNanos(budget), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            log.warn("{} in-flight exports did not settle within the shutdown deadline", snapshot.size());
            return e;
        } catch (ExecutionException ignored) {
            // A failed export is drained just like a successful one; its result is observed by the
            // caller and the client can still release all transport resources.
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return e;
        }
        return null;
    }

    /// Closes the [ClientFactory] within the remaining `budgetNanos`, so the returned shutdown stage
    /// honours the caller's deadline even when a slow or unreachable collector stalls connection
    /// teardown. Armeria's own `close()` is unbounded and not usefully interruptible, so this bounds
    /// the wait on the async close and abandons it on timeout (the orphaned close finishes in Armeria's
    /// own threads) rather than blocking the shutdown stage past the deadline.
    private @Nullable Throwable closeFactoryWithin(long budgetNanos) {
        if (clientFactory == null) {
            return null;
        }
        try {
            clientFactory.closeAsync().get(budgetNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            log.warn("client factory did not close within the shutdown deadline; abandoning the close");
            return e;
        } catch (ExecutionException e) {
            return e.getCause() != null ? e.getCause() : e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return e;
        }
        return null;
    }

    /// One signal's export: reads the payload's wire representation, sends it asynchronously, and maps
    /// the response to a [DeliveryResult].
    @FunctionalInterface
    private interface Export {
        CompletionStage<DeliveryResult> result(Payload<?> payload, Provenance provenance);
    }
}
