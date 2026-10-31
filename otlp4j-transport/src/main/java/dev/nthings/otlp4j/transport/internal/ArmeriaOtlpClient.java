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
import com.linecorp.armeria.common.ContentTooLargeException;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.grpc.protocol.ArmeriaStatusException;
import com.linecorp.armeria.common.logging.RequestLog;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.codec.PayloadWire;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.Compression;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.transport.internal.ClientHeaders;
import dev.nthings.otlp4j.transport.internal.ExporterMetrics;
import io.grpc.Metadata;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
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
/// Every wire outcome completes with a typed [DeliveryResult]; a wire failure never completes the
/// stage exceptionally. What survives the retries is classified with the same decision functions the
/// retry rule used ([ArmeriaRetryRules]), so the result tells the caller whether re-sending can help:
/// a non-2xx HTTP status, a gRPC status with its pushback and `RetryInfo`, and a transport failure
/// (refused, reset, closed session) are retryable or permanent downstream rejections, carrying the
/// server's retry-after hint when the policy respects it, and the total deadline expiring is a
/// retryable `TIMEOUT`. A failure preparing the request (oversize, invalid headers, marshalling) is
/// permanent and not downstream, since no retry can change it. A 2xx response is success: one whose
/// body cannot be read as the OTLP response is accepted when its content type says it is not
/// protobuf, and permanent when it claims to be protobuf yet does not parse, because the server has
/// already accepted the data and a retry would duplicate it. Only caller cancellation and an exporter
/// that was never started remain exceptional.
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
    /// `provenance`. Never throws synchronously, and the stage completes exceptionally only for an
    /// exporter that was never started or a cancelled caller: every other outcome, including a failure
    /// preparing the request and a client that is stopping, is a typed result.
    ///
    /// @param payload    the payload to export
    /// @param provenance the provenance to attribute the result to
    /// @return a stage completing with the export outcome
    public <T> CompletionStage<DeliveryResult> export(Payload<T> payload, Provenance provenance) {
        synchronized (this) {
            if (closing) {
                return CompletableFuture.completedFuture(
                        DeliveryResult.shuttingDown("exporter is stopping", provenance));
            }
            if (!started) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("exporter has not been started"));
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
                // Everything that can throw here runs before a byte is sent: marshalling, the size
                // cap, and header resolution. The same request would fail the same way again, so
                // this is permanent, and it is not the endpoint's doing.
                return CompletableFuture.completedFuture(
                        DeliveryResult.permanentFrom("failed to prepare the OTLP request", e, provenance));
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
            return withRetryAccounting(provenance, cancel -> {
                var response = toCompletableFuture(rpc.apply(request));
                cancel.set(() -> response.cancel(true));
                var mapped = response.thenApply(value -> codec.deliveryResult(value, provenance));
                return mapped;
            });
        };
    }

    /// Binds a [SignalCodec] to an OTLP/HTTP POST at `path`: serialises the payload, posts it (retried
    /// by the installed decorator), and maps the final response to a [DeliveryResult].
    private <T, REQ extends MessageLite, RESP extends MessageLite> Export httpExport(
            WebClient webClient, SignalCodec<T, REQ, RESP> codec, String path) {
        return (payload, provenance) -> {
            var body = serialize(payload);
            return withRetryAccounting(provenance, cancel -> {
                var response = webClient.execute(request(path, body));
                // Abort the public response stream as well as the captured request context. Armeria
                // wrappers may delegate one operation but not the other depending on the installed
                // retry decorator, so retaining both paths keeps cancellation effective for HTTP/1
                // and HTTP/2.
                cancel.set(response::abort);
                return response.aggregate().thenApply(result -> httpResult(codec, path, result, provenance));
            });
        };
    }

    /// Maps a final HTTP response to a [DeliveryResult]. A 2xx is success; the status, not the body,
    /// is what the server committed to, so a body that is empty or not declared as protobuf is simply
    /// accepted (as the OpenTelemetry Collector's exporter does). A body that claims to be protobuf
    /// but does not parse is permanent: the data was accepted, so a retry would only duplicate it.
    /// Any other status is classified by the retry rule's own decision.
    private <T, REQ extends MessageLite, RESP extends MessageLite> DeliveryResult httpResult(
            SignalCodec<T, REQ, RESP> codec, String path, AggregatedHttpResponse response, Provenance provenance) {
        var code = response.status().code();
        if (code >= 200 && code < 300) {
            if (response.content().isEmpty() || !isProtobuf(response.contentType())) {
                return DeliveryResult.accepted(provenance);
            }
            RESP parsed;
            try {
                parsed = codec.parseResponse(response.content().array());
            } catch (InvalidProtocolBufferException e) {
                return DeliveryResult.downstream(
                        Disposition.PERMANENT, DeliveryReason.LOCAL_FAILURE,
                        "OTLP/HTTP export to " + path + " returned HTTP " + code
                                + " with an unreadable protobuf response body: " + e.getMessage(),
                        e, null, provenance);
            }
            return codec.deliveryResult(parsed, provenance);
        }
        var decision = ArmeriaRetryRules.decideHttp(
                code, response.headers().get(ArmeriaRetryRules.RETRY_AFTER), config.retry().respectServerRetryAfter());
        return ArmeriaRetryRules.toDelivery(decision, rejectionMessage(path, response), null, provenance);
    }

    /// Whether `contentType` is the OTLP/HTTP binary protobuf media type, ignoring parameters.
    private static boolean isProtobuf(@Nullable MediaType contentType) {
        return contentType != null
                && "application".equalsIgnoreCase(contentType.type())
                && "x-protobuf".equalsIgnoreCase(contentType.subtype());
    }

    /// Initiates `call` under a captured client context and returns a stage that settles once the
    /// delivery has settled and its retries have been counted. The request is initiated synchronously
    /// (the async stub / web client returns a future immediately), so the captor observes its context
    /// before it is closed. The returned stage completes only after the retry instrument has been
    /// written, so a caller that observes completion also observes the retry count. A failure of the
    /// wire call settles as the typed result [#wireFailure] classifies, never exceptionally; only a
    /// cancellation stays exceptional. The per-attempt response timeout never surfaces here, since the
    /// retrying client consumes it as a failed attempt and retries; a total-deadline response timeout
    /// is a retryable `TIMEOUT`.
    ///
    /// The returned stage carries a total-budget timeout: gating completion on the retry-log
    /// settling means the caller-observed completion can otherwise lag the wire response timeout while
    /// a timed-out request against an unresponsive peer finishes settling its log, so this bounds the
    /// caller's wait on the total budget independently of that settling. It is a scheduled timer, not a
    /// carrier thread, so it keeps the per-export thread hop removed. When the stage settles before the
    /// wire call does (that timeout, or the caller's own deadline or cancellation) the wire call is
    /// cancelled too, otherwise an abandoned export would stay in flight until the peer answered.
    private CompletionStage<DeliveryResult> withRetryAccounting(
            Provenance provenance, Function<WireCancellation, CompletableFuture<DeliveryResult>> call) {
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
                    } else if (isCancellation(error)) {
                        settled.completeExceptionally(error);
                    } else {
                        settled.complete(wireFailure(error, provenance));
                    }
                }));
        settled.completeOnTimeout(
                totalDeadlineExceeded(provenance), timeoutNanos(config.timeout().total()), TimeUnit.NANOSECONDS);
        settled.whenComplete((result, error) -> {
            // The stage can only be unfinished here if something other than the wire settled
            // `settled`: the total-budget timeout above, a caller's own timeout, or a cancellation.
            // CompletableFuture timeouts only settle the dependent stage, so cancel the active Armeria
            // request as well, otherwise it would stay in flight until the remote peer answers.
            if (settled.isCancelled() || !stage.isDone()) {
                cancelWire(stage, contexts, cancellation);
            }
        });
        return settled;
    }

    /// The result of an export whose total deadline elapsed: a retryable `TIMEOUT` attributed
    /// downstream, since the endpoint (or the path to it) did not answer in time.
    ///
    /// @param provenance the exporter's provenance
    /// @return the timeout rejection
    public static DeliveryResult totalDeadlineExceeded(Provenance provenance) {
        return timedOut(null, provenance);
    }

    private static DeliveryResult timedOut(@Nullable Throwable cause, Provenance provenance) {
        return DeliveryResult.downstream(
                Disposition.RETRYABLE, DeliveryReason.TIMEOUT,
                "OTLP export exceeded the total deadline", cause, null, provenance);
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

    /// Classifies a failure of the wire call that survived the retrying client. A timeout comes first,
    /// since Armeria may wrap it in a gRPC `DEADLINE_EXCEEDED` whose status alone would hide the cause.
    /// A gRPC status is decided from its code, `grpc-retry-pushback-ms` and `RetryInfo` by the retry
    /// rule's own function. A response that exceeds the size cap is permanent, as a retry would meet the
    /// same response. Every other failure is a transport failure (refused, reset, closed session, TLS):
    /// retryable, because nothing says the endpoint rejected the data.
    private DeliveryResult wireFailure(Throwable error, Provenance provenance) {
        var causes = causeChain(error);
        for (var cause : causes) {
            if (cause instanceof ResponseTimeoutException) {
                return timedOut(cause, provenance);
            }
        }
        var respectHints = config.retry().respectServerRetryAfter();
        for (var cause : causes) {
            // Armeria's own gRPC limits (a message over the configured size) surface as a CANCELLED
            // status whose cause carries the real code; the code, not the wrapper, decides.
            if (cause instanceof ArmeriaStatusException armeria) {
                return ArmeriaRetryRules.toDelivery(
                        ArmeriaRetryRules.decideGrpc(armeria.getCode(), null, null, false, respectHints),
                        "OTLP/gRPC export failed: " + armeria.getMessage(), error, provenance);
            }
        }
        for (var cause : causes) {
            var status = cause instanceof StatusRuntimeException runtime
                    ? runtime.getStatus()
                    : cause instanceof StatusException checked ? checked.getStatus() : null;
            if (status != null) {
                var trailers = cause instanceof StatusRuntimeException runtime
                        ? runtime.getTrailers()
                        : ((StatusException) cause).getTrailers();
                var message = "OTLP/gRPC export rejected: " + status.getCode()
                        + (status.getDescription() == null ? "" : " - " + status.getDescription());
                return ArmeriaRetryRules.toDelivery(
                        ArmeriaRetryRules.decideGrpc(status, trailers, respectHints), message, error, provenance);
            }
        }
        for (var cause : causes) {
            if (cause instanceof ContentTooLargeException) {
                return DeliveryResult.downstream(
                        Disposition.PERMANENT, DeliveryReason.LOCAL_FAILURE,
                        "OTLP export response exceeds the configured maxResponseBytes", error, null, provenance);
            }
        }
        return ArmeriaRetryRules.toDelivery(
                ArmeriaRetryRules.decideHttp(null, true),
                "OTLP export failed in transit: " + rootMessage(error), error, provenance);
    }

    private static boolean isCancellation(Throwable error) {
        return causeChain(error).stream().anyMatch(CancellationException.class::isInstance);
    }

    private static List<Throwable> causeChain(Throwable error) {
        var chain = new ArrayList<Throwable>();
        for (Throwable cause = error; cause != null && !chain.contains(cause); cause = cause.getCause()) {
            chain.add(cause);
        }
        return chain;
    }

    private static String rootMessage(Throwable error) {
        var chain = causeChain(error);
        var root = chain.getLast();
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
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

    private static String rejectionMessage(String path, AggregatedHttpResponse response) {
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
        return "OTLP/HTTP export to " + path + " rejected: HTTP " + code + (text.isEmpty() ? "" : " - " + text);
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
