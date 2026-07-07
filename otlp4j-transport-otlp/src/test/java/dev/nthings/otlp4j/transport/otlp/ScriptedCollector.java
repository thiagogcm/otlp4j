package dev.nthings.otlp4j.transport.otlp;

import com.google.protobuf.Any;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/// A small in-module fake OTLP collector, built on Armeria, that replays a per-attempt [Step] script
/// and records each attempt's arrival so a retry-policy test can read the attempt count and the gaps
/// between attempts. It lives inside the transport module (not the classpath-only `otlp4j-testing`
/// fixtures) so the generated proto service base classes are reachable through the module's own
/// qualified proto export, avoiding the named-module-versus-classpath access wall that keeps the
/// policy-suite seam in the conformance module.
///
/// One collector serves the trace signal only (the retry cases drive traces), over OTLP/gRPC or
/// OTLP/HTTP as selected by [Wire]. Beyond the scripted length it answers a default retryable
/// rejection and still records the arrival, so a straggler cannot stall the server or corrupt the
/// count.
final class ScriptedCollector implements AutoCloseable {

    /// The wire protocol the collector serves.
    enum Wire { GRPC, HTTP }

    /// One scripted per-attempt behaviour.
    sealed interface Step {
        /// A full success.
        record Ok() implements Step {}
        /// A retryable rejection with no server hint (gRPC `UNAVAILABLE`, HTTP `503`).
        record Unavailable() implements Step {}
        /// A retryable rejection carrying a positive backoff hint (gRPC pushback trailer, HTTP numeric
        /// `Retry-After` in whole seconds).
        record Throttle(Duration hint) implements Step {}
        /// A stop-retry directive (gRPC negative pushback, HTTP a permanent 400).
        record StopRetry() implements Step {}
        /// A retryable rejection carrying a gRPC `RetryInfo` detail (gRPC only).
        record RetryInfo(Duration hint) implements Step {}
        /// Holds the attempt open for `duration` before answering OK.
        record Hang(Duration duration) implements Step {}
    }

    private static final Metadata.Key<String> PUSHBACK_MS =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);

    private final Server server;
    private final List<Step> script;
    private final List<Long> arrivalNanos = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger cursor = new AtomicInteger();

    private ScriptedCollector(Server server, List<Step> script) {
        this.server = server;
        this.script = List.copyOf(script);
    }

    /// Starts a collector on an ephemeral loopback port serving `wire` with `script`.
    static ScriptedCollector start(Wire wire, List<Step> script) {
        var holder = new ScriptedCollector[1];
        var sb = Server.builder().http(0);
        switch (wire) {
            case GRPC -> {
                var service = new TraceServiceGrpc.TraceServiceImplBase() {
                    @Override
                    public void export(
                            ExportTraceServiceRequest request,
                            StreamObserver<ExportTraceServiceResponse> observer) {
                        holder[0].answerGrpc(observer);
                    }
                };
                sb.service(GrpcService.builder().addService(service).useBlockingTaskExecutor(true).build());
            }
            case HTTP -> sb.service("/v1/traces", (ctx, req) ->
                    HttpResponse.of(req.aggregate().thenApply(ignored -> holder[0].answerHttp())));
        }
        var server = sb.build();
        server.start().join();
        var collector = new ScriptedCollector(server, script);
        holder[0] = collector;
        return collector;
    }

    private Step recordAndNext() {
        arrivalNanos.add(System.nanoTime());
        int index = cursor.getAndIncrement();
        return index < script.size() ? script.get(index) : new Step.Unavailable();
    }

    private void answerGrpc(StreamObserver<ExportTraceServiceResponse> observer) {
        var step = recordAndNext();
        try {
            switch (step) {
                case Step.Ok ignored -> grpcOk(observer);
                case Step.Unavailable ignored -> observer.onError(
                        Status.UNAVAILABLE.withDescription("unavailable").asRuntimeException());
                case Step.Throttle t -> observer.onError(grpcPushback(t.hint().toMillis()));
                case Step.StopRetry ignored -> observer.onError(grpcPushback(-1));
                case Step.RetryInfo r -> observer.onError(grpcRetryInfo(r.hint()));
                case Step.Hang hang -> {
                    sleep(hang.duration());
                    grpcOk(observer);
                }
            }
        } catch (RuntimeException ignored) {
            // the client may have abandoned the call at its deadline; ignore a late close
        }
    }

    private HttpResponse answerHttp() {
        var step = recordAndNext();
        return switch (step) {
            case Step.Ok ignored -> httpOk();
            case Step.Unavailable ignored -> HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
            case Step.Throttle t -> HttpResponse.of(ResponseHeaders.builder(HttpStatus.SERVICE_UNAVAILABLE)
                    .add(HttpHeaderNames.RETRY_AFTER, Long.toString(Math.max(1, t.hint().toSeconds())))
                    .build());
            case Step.StopRetry ignored -> HttpResponse.of(HttpStatus.BAD_REQUEST);
            // RetryInfo is a gRPC detail; on HTTP degrade to a plain retryable rejection.
            case Step.RetryInfo ignored -> HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
            case Step.Hang hang -> HttpResponse.of(CompletableFuture.supplyAsync(
                    this::httpOk, CompletableFuture.delayedExecutor(
                            hang.duration().toMillis(), TimeUnit.MILLISECONDS)));
        };
    }

    private static void grpcOk(StreamObserver<ExportTraceServiceResponse> observer) {
        observer.onNext(ExportTraceServiceResponse.getDefaultInstance());
        observer.onCompleted();
    }

    private static io.grpc.StatusRuntimeException grpcPushback(long millis) {
        var trailers = new Metadata();
        trailers.put(PUSHBACK_MS, Long.toString(millis));
        return Status.UNAVAILABLE.withDescription("throttle").asRuntimeException(trailers);
    }

    private static io.grpc.StatusRuntimeException grpcRetryInfo(Duration hint) {
        var delay = com.google.protobuf.Duration.newBuilder()
                .setSeconds(hint.getSeconds())
                .setNanos(hint.getNano())
                .build();
        var detail = com.google.rpc.RetryInfo.newBuilder().setRetryDelay(delay).build();
        var status = com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.UNAVAILABLE.value())
                .setMessage("retryinfo")
                .addDetails(Any.pack(detail))
                .build();
        return StatusProto.toStatusRuntimeException(status);
    }

    private HttpResponse httpOk() {
        return HttpResponse.of(HttpStatus.OK, MediaType.PROTOBUF,
                ExportTraceServiceResponse.getDefaultInstance().toByteArray());
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(Math.max(0, d.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    int port() {
        return server.activeLocalPort();
    }

    int attempts() {
        synchronized (arrivalNanos) {
            return arrivalNanos.size();
        }
    }

    List<Duration> interAttemptGaps() {
        synchronized (arrivalNanos) {
            var gaps = new ArrayList<Duration>(Math.max(0, arrivalNanos.size() - 1));
            for (int i = 1; i < arrivalNanos.size(); i++) {
                gaps.add(Duration.ofNanos(arrivalNanos.get(i) - arrivalNanos.get(i - 1)));
            }
            return List.copyOf(gaps);
        }
    }

    @Override
    public void close() {
        server.stop().join();
    }
}
