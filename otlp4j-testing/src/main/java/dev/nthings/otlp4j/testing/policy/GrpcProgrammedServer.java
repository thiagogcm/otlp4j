package dev.nthings.otlp4j.testing.policy;

import com.google.protobuf.Any;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/// The OTLP/gRPC variant of [ProgrammedOtlpServer], serving the trace export service over grpc-netty.
/// It maps each neutral [ProgrammedResponse] to the gRPC wire the client already reads: a full success
/// is an OK response, a retryable rejection is `UNAVAILABLE`, a throttle carries a positive
/// `grpc-retry-pushback-ms` trailer, a stop-retrying throttle carries a negative one, a RetryInfo
/// rejection carries a `grpc-status-details-bin` RetryInfo detail, a permanent rejection is
/// `INVALID_ARGUMENT`, and a hang sleeps before answering so the client's per-call deadline fires.
///
/// The policy suite drives one signal (traces), so only the trace service is served.
final class GrpcProgrammedServer extends ProgrammedOtlpServer {

    /// The gRPC server-pushback trailer name, the standard OTLP `grpc-retry-pushback-ms` wire trailer.
    /// The consolidated transport reads and writes the same trailer through its own internal constant,
    /// which sits below otlp4j-testing in the dependency graph and so cannot be referenced here. A
    /// rename of the wire value is therefore a deliberate two-place edit; grep for this literal to find
    /// both.
    static final String GRPC_RETRY_PUSHBACK_MS = "grpc-retry-pushback-ms";

    private static final Metadata.Key<String> PUSHBACK_KEY =
            Metadata.Key.of(GRPC_RETRY_PUSHBACK_MS, Metadata.ASCII_STRING_MARSHALLER);

    private final Server server;

    private GrpcProgrammedServer(List<ProgrammedResponse> script, Server server) {
        super(script);
        this.server = server;
    }

    static GrpcProgrammedServer started(List<ProgrammedResponse> script) {
        var holder = new GrpcProgrammedServer[1];
        var service = new TraceServiceGrpc.TraceServiceImplBase() {
            @Override
            public void export(
                    ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> observer) {
                holder[0].answer(request, observer);
            }
        };
        try {
            var server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                    .addService(service)
                    .build()
                    .start();
            var instance = new GrpcProgrammedServer(script, server);
            holder[0] = instance;
            return instance;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start the programmed OTLP/gRPC server", e);
        }
    }

    private void answer(
            ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> observer) {
        var response = recordAndNext();
        try {
            switch (response) {
                case ProgrammedResponse.Ok ignored -> ok(observer);
                case ProgrammedResponse.Unavailable ignored ->
                        observer.onError(Status.UNAVAILABLE.withDescription("programmed unavailable").asRuntimeException());
                case ProgrammedResponse.Throttle throttle -> observer.onError(pushback(throttle.retryAfter().toMillis()));
                // The date-form throttle is HTTP-specific; on gRPC it degrades to a numeric pushback.
                case ProgrammedResponse.ThrottleDate throttle -> observer.onError(pushback(throttle.retryAfter().toMillis()));
                case ProgrammedResponse.ThrottleNoRetry ignored -> observer.onError(pushback(-1));
                case ProgrammedResponse.RetryInfo retryInfo -> observer.onError(retryInfo(retryInfo.retryAfter()));
                case ProgrammedResponse.Permanent ignored ->
                        observer.onError(Status.INVALID_ARGUMENT.withDescription("programmed permanent").asRuntimeException());
                case ProgrammedResponse.Hang hang -> {
                    sleep(hang.duration());
                    ok(observer);
                }
            }
        } catch (RuntimeException e) {
            // The client may have abandoned the call at its deadline before this handler answered;
            // swallow the resulting "call already closed" so teardown is not disturbed.
        }
    }

    private static void ok(StreamObserver<ExportTraceServiceResponse> observer) {
        observer.onNext(ExportTraceServiceResponse.getDefaultInstance());
        observer.onCompleted();
    }

    /// A retryable `UNAVAILABLE` carrying a `grpc-retry-pushback-ms` trailer. A negative value tells the
    /// client to stop retrying; a positive value is a backoff hint.
    private static io.grpc.StatusRuntimeException pushback(long millis) {
        var trailers = new Metadata();
        trailers.put(PUSHBACK_KEY, Long.toString(millis));
        return Status.UNAVAILABLE.withDescription("programmed throttle").asRuntimeException(trailers);
    }

    /// A retryable rejection carrying a `grpc-status-details-bin` RetryInfo detail. The legacy client
    /// reads only the pushback trailer, so it ignores this detail; a transport that folds RetryInfo will
    /// honour it.
    private static io.grpc.StatusRuntimeException retryInfo(Duration retryAfter) {
        var delay = com.google.protobuf.Duration.newBuilder()
                .setSeconds(retryAfter.getSeconds())
                .setNanos(retryAfter.getNano())
                .build();
        var detail = com.google.rpc.RetryInfo.newBuilder().setRetryDelay(delay).build();
        var status = com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.UNAVAILABLE.value())
                .setMessage("programmed retryinfo")
                .addDetails(Any.pack(detail))
                .build();
        return StatusProto.toStatusRuntimeException(status);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(Math.max(0, d.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while holding a programmed attempt open", e);
        }
    }

    @Override
    public int port() {
        return server.getPort();
    }

    @Override
    public void close() {
        server.shutdownNow();
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
