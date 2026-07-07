package dev.nthings.otlp4j.transport.grpc.internal;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.transport.common.ClientHeaders;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.transport.common.internal.ResilienceRetries;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.github.resilience4j.retry.Retry;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The OTLP/gRPC implementation of the [OtlpClient] SPI: opens a channel to a collector
/// endpoint and exports domain telemetry asynchronously.
///
/// Honours the full [ClientConfig]: TLS credentials, per-call headers, gzip, and retry.
///
/// **Internal.** Part of the transport layer; obtained via the SPI.
public final class GrpcOtlpClient implements OtlpClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcOtlpClient.class);

    /// Runs each blocking teardown wait on a virtual thread, mirroring `GrpcOtlpServer`'s naming
    /// precedent so a stalled teardown is greppable by thread name.
    private static final Executor SHUTDOWN_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-grpc-client-shutdown").start(task);

    private final ManagedChannel channel;
    private final TraceServiceGrpc.TraceServiceBlockingStub traceStub;
    private final MetricsServiceGrpc.MetricsServiceBlockingStub metricsStub;
    private final LogsServiceGrpc.LogsServiceBlockingStub logsStub;
    private final ProfilesServiceGrpc.ProfilesServiceBlockingStub profilesStub;
    private final ClientConfig config;
    private final boolean compress;
    private final ExecutorService executor;
    private final Retry retry;
    private final Map<SignalDescriptor<?>, GrpcExport<?, ?, ?>> exports = new HashMap<>();

    public GrpcOtlpClient(ClientConfig config) {
        this.config = config;
        this.compress = config.compression() == Compression.GZIP;

        var builder = Grpc.newChannelBuilderForAddress(
                config.host(), config.port(), channelCredentials(config.tls()));
        if (config.headerSupplier() != null) {
            builder.intercept(new DynamicHeadersInterceptor(config.headers(), config.headerSupplier()));
        } else if (!config.headers().isEmpty()) {
            builder.intercept(MetadataUtils.newAttachHeadersInterceptor(toMetadata(config.headers())));
        }
        this.channel = builder.build();

        this.traceStub = TraceServiceGrpc.newBlockingStub(channel);
        this.metricsStub = MetricsServiceGrpc.newBlockingStub(channel);
        this.logsStub = LogsServiceGrpc.newBlockingStub(channel);
        this.profilesStub = ProfilesServiceGrpc.newBlockingStub(channel);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.retry = ResilienceRetries.create(
                "otlp-grpc",
                config.retry(),
                GrpcOtlpClient::isRetryable,
                GrpcOtlpClient::retryAfter);

        // Per-signal binding: each entry pairs a SignalCodec with the blocking-stub RPC. Populated
        // once here so export() is a single map lookup instead of four typed methods.
        exports.put(SignalDescriptor.TRACES, new GrpcExport<>(
                SignalCodec.TRACES, request -> call(traceStub).export(request)));
        exports.put(SignalDescriptor.METRICS, new GrpcExport<>(
                SignalCodec.METRICS, request -> call(metricsStub).export(request)));
        exports.put(SignalDescriptor.LOGS, new GrpcExport<>(
                SignalCodec.LOGS, request -> call(logsStub).export(request)));
        exports.put(SignalDescriptor.PROFILES, new GrpcExport<>(
                SignalCodec.PROFILES, request -> call(profilesStub).export(request)));

        log.debug("opened OTLP/gRPC channel to {}:{}", config.host(), config.port());
    }

    @Override
    public <T> CompletionStage<DeliveryResult> export(
            SignalDescriptor<T> signal, T batch, Provenance provenance) {
        // Safe: exports is populated at construction with one entry per signal, keyed by descriptor
        // identity, whose value type parameter is the same T carried by the signal.
        @SuppressWarnings("unchecked")
        var export = (GrpcExport<T, ?, ?>) exports.get(signal);
        // Double-deadline, belt-and-suspenders and intentional: the stub's withDeadlineAfter (in
        // call(...)) bounds each in-flight RPC, while this orTimeout bounds the whole async attempt
        // (retry backoff and executor queueing included), so a stalled carrier thread cannot
        // outlive the caller's budget. A slow call is interrupted by whichever fires first.
        return CompletableFuture.supplyAsync(
                () -> ResilienceRetries.execute(retry, () -> export.result(batch, provenance)),
                executor)
                .orTimeout(config.timeout().toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Binds a [SignalCodec] to its blocking-stub RPC for one signal: encodes the batch, invokes the
    /// unary RPC, and interprets the response as a [DeliveryResult]. The four gRPC stubs stay
    /// gRPC-specific, so this wrapper is transport-local.
    private record GrpcExport<T, REQ extends MessageLite, RESP extends MessageLite>(
            SignalCodec<T, REQ, RESP> codec, Function<REQ, RESP> rpc) {

        DeliveryResult result(T batch, Provenance provenance) {
            return codec.deliveryResult(rpc.apply(codec.toProto(batch)), provenance);
        }
    }

    /// Applies the per-call deadline and optional gzip compression to a stub.
    private <S extends AbstractStub<S>> S call(S stub) {
        var s = stub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS);
        return compress ? s.withCompression("gzip") : s;
    }

    /// Two-phase teardown on a named shutdown carrier, bounded so the total wall-clock cannot
    /// exceed `timeout`. One deadline is computed up front; each phase (channel termination, then
    /// per-export executor drain) receives the REMAINING budget clamped at zero via
    /// `Math.max(0, ...)`, so the worst case is `timeout`, never `2 x timeout`. Both awaits are
    /// interrupt-responsive: an interrupt in phase one leaves the flag set, so phase two's await
    /// returns at once, and teardown still forces both resources down so no export carrier leaks.
    /// `forceFlush` is the inherited no-op (the client does not buffer).
    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            var deadline = System.nanoTime() + timeout.toNanos();
            channel.shutdown();
            awaitChannelTermination(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            drainExecutor(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }, SHUTDOWN_EXECUTOR);
    }

    /// Awaits channel termination within `budget`, forcing it down on timeout or interrupt.
    private void awaitChannelTermination(Duration budget) {
        try {
            if (!channel.awaitTermination(budget.toNanos(), TimeUnit.NANOSECONDS)) {
                log.warn("OTLP/gRPC channel did not terminate within {}; forcing shutdown", budget);
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            // Interrupt unblocks this await; force the channel down and re-set the flag so the
            // executor drain that follows returns immediately.
            log.warn("interrupted while closing OTLP/gRPC channel; forcing shutdown");
            channel.shutdownNow();
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

    private static final Set<Status.Code> RETRYABLE_STATUS_CODES = Set.of(
            Status.Code.CANCELLED,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.RESOURCE_EXHAUSTED,
            Status.Code.ABORTED,
            Status.Code.OUT_OF_RANGE,
            Status.Code.UNAVAILABLE,
            Status.Code.DATA_LOSS);

    private static boolean isRetryable(Throwable error) {
        return !retryPushbackDisablesRetry(error)
                && RETRYABLE_STATUS_CODES.contains(Status.fromThrowable(error).getCode());
    }

    private static Duration retryAfter(Throwable error) {
        var trailers = Status.trailersFromThrowable(error);
        if (trailers == null) {
            return Duration.ZERO;
        }
        var pushback = trailers.get(GrpcPushback.PUSHBACK_MS);
        if (pushback == null) {
            return Duration.ZERO;
        }
        try {
            var millis = Long.parseLong(pushback);
            return millis > 0 ? Duration.ofMillis(millis) : Duration.ZERO;
        } catch (NumberFormatException _) {
            return Duration.ZERO;
        }
    }

    private static boolean retryPushbackDisablesRetry(Throwable error) {
        var trailers = Status.trailersFromThrowable(error);
        if (trailers == null) {
            return false;
        }
        var pushback = trailers.get(GrpcPushback.PUSHBACK_MS);
        if (pushback == null) {
            return false;
        }
        try {
            return Long.parseLong(pushback) < 0;
        } catch (NumberFormatException _) {
            return false;
        }
    }

    private static ChannelCredentials channelCredentials(Tls tls) {
        return switch (tls) {
            case Tls.Disabled() -> InsecureChannelCredentials.create();
            case Tls.SystemTrust() -> TlsChannelCredentials.create();
            case Tls.Custom(var certFile, var keyFile, var trustFile) -> {
                Transports.requireCompleteClientMutualTls(certFile, keyFile);
                try {
                    var b = TlsChannelCredentials.newBuilder();
                    if (certFile != null && keyFile != null) {
                        b.keyManager(certFile.toFile(), keyFile.toFile());
                    }
                    if (trustFile != null) {
                        b.trustManager(trustFile.toFile());
                    }
                    yield b.build();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to load client TLS material", e);
                }
            }
            case Tls.Inline(var cert, var key, var trust) -> {
                Transports.requireCompleteClientMutualTls(cert, key);
                try {
                    var b = TlsChannelCredentials.newBuilder();
                    if (cert != null && key != null) {
                        b.keyManager(new ByteArrayInputStream(cert), new ByteArrayInputStream(key));
                    }
                    if (trust != null) {
                        b.trustManager(new ByteArrayInputStream(trust));
                    }
                    yield b.build();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to load client TLS material", e);
                }
            }
            // gRPC has no SSLContext door: honour the trust manager only (no mTLS via SSLContext).
            case Tls.SslContext(var _, var trustManager) ->
                TlsChannelCredentials.newBuilder().trustManager(trustManager).build();
        };
    }

    private static Metadata toMetadata(Map<String, String> headers) {
        var md = new Metadata();
        headers.forEach((name, value) ->
                md.put(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER), value));
        return md;
    }

    /// Attaches per-call headers, re-evaluating the header supplier on every RPC so a rotating
    /// credential is refreshed. Installed only when a supplier is set; the static-header path keeps
    /// the cheaper attach-headers interceptor.
    private static final class DynamicHeadersInterceptor implements ClientInterceptor {

        private final Map<String, String> staticHeaders;
        private final Supplier<Map<String, String>> headerSupplier;

        DynamicHeadersInterceptor(Map<String, String> staticHeaders, Supplier<Map<String, String>> headerSupplier) {
            this.staticHeaders = staticHeaders;
            this.headerSupplier = headerSupplier;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    headers.merge(toMetadata(ClientHeaders.resolve(staticHeaders, headerSupplier)));
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
