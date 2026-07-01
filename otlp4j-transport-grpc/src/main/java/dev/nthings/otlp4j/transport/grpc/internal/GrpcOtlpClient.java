package dev.nthings.otlp4j.transport.grpc.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.codec.LogsMapper;
import dev.nthings.otlp4j.codec.MetricsMapper;
import dev.nthings.otlp4j.codec.ProfilesMapper;
import dev.nthings.otlp4j.codec.TraceMapper;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.config.Tls;
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
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final ManagedChannel channel;
    private final TraceServiceGrpc.TraceServiceBlockingStub traceStub;
    private final MetricsServiceGrpc.MetricsServiceBlockingStub metricsStub;
    private final LogsServiceGrpc.LogsServiceBlockingStub logsStub;
    private final ProfilesServiceGrpc.ProfilesServiceBlockingStub profilesStub;
    private final ClientConfig config;
    private final boolean compress;
    private final ExecutorService executor;

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
        var retry = config.retry();
        if (retry.maxAttempts() > 1) {
            builder.enableRetry()
                    .maxRetryAttempts(retry.maxAttempts())
                    .defaultServiceConfig(RetryServiceConfig.build(retry, OTLP_SERVICE_NAMES));
        }
        this.channel = builder.build();

        this.traceStub = TraceServiceGrpc.newBlockingStub(channel);
        this.metricsStub = MetricsServiceGrpc.newBlockingStub(channel);
        this.logsStub = LogsServiceGrpc.newBlockingStub(channel);
        this.profilesStub = ProfilesServiceGrpc.newBlockingStub(channel);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        log.debug("opened OTLP/gRPC channel to {}:{}", config.host(), config.port());
    }

    @Override
    public CompletionStage<ConsumeResult> exportTraces(TracesData traces) {
        return CompletableFuture.supplyAsync(
                () -> TraceMapper.result(call(traceStub).export(TraceMapper.toProto(traces))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult> exportMetrics(MetricsData metrics) {
        return CompletableFuture.supplyAsync(
                () -> MetricsMapper.result(call(metricsStub).export(MetricsMapper.toProto(metrics))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult> exportLogs(LogsData logs) {
        return CompletableFuture.supplyAsync(
                () -> LogsMapper.result(call(logsStub).export(LogsMapper.toProto(logs))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult> exportProfiles(ProfilesData profiles) {
        return CompletableFuture.supplyAsync(
                () -> ProfilesMapper.result(call(profilesStub).export(ProfilesMapper.toProto(profiles))), executor);
    }

    /// Applies the per-call deadline and optional gzip compression to a stub.
    private <S extends AbstractStub<S>> S call(S stub) {
        var s = stub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS);
        return compress ? s.withCompression("gzip") : s;
    }

    /// Two-phase teardown bounded by [#CLOSE_TIMEOUT_SECONDS] per await. Both awaits are
    /// interrupt-responsive: an interrupt propagates through both phases so teardown stops
    /// promptly when the caller's deadline elapses.
    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("OTLP/gRPC channel did not terminate within {}s; forcing shutdown",
                        CLOSE_TIMEOUT_SECONDS);
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            // Interrupt unblocks this await; force channel down. The interrupt flag stays set so
            // the executor await in the finally block returns immediately.
            log.warn("interrupted while closing OTLP/gRPC channel; forcing shutdown");
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            // Bound teardown: shut down, wait, then force so no export carriers leak.
            executor.shutdown();
            try {
                if (!executor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.warn("per-export executor did not terminate within {}s; interrupting outstanding exports",
                            CLOSE_TIMEOUT_SECONDS);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /// OTLP collector service names used to scope the retry service config.
    private static final List<String> OTLP_SERVICE_NAMES = List.of(
            TraceServiceGrpc.SERVICE_NAME,
            MetricsServiceGrpc.SERVICE_NAME,
            LogsServiceGrpc.SERVICE_NAME,
            ProfilesServiceGrpc.SERVICE_NAME);

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
                    headers.merge(toMetadata(Transports.resolveHeaders(staticHeaders, headerSupplier)));
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
