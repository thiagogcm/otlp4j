package dev.nthings.otlp4j.testing.conformance;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.TlsChannelCredentials;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/// A black-box OTLP/gRPC driver built on grpc-java's generated blocking stubs.
///
/// It builds a channel from the gateway target with plaintext, server-auth, or mutual TLS
/// credentials, drives the four export services with minimal or oversized requests, and can gzip a
/// request through `withCompression("gzip")`. On failure it captures the [Status] and the
/// `grpc-retry-pushback-ms` trailer, both surfaced through [WireResponse]. It never touches
/// otlp4j-codec, keeping the suite black-box at the wire boundary.
public final class GrpcWireClient implements AutoCloseable {

    private static final Metadata.Key<String> PUSHBACK_MS =
            Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER);

    private final ManagedChannel channel;

    /// Creates a client against the gateway gRPC target with the given TLS posture.
    ///
    /// @param target the gateway target, for example `127.0.0.1:53413`
    /// @param tls the TLS posture to take
    /// @param serverTrustPem the PEM to trust as the server CA, or null for a plaintext scenario
    public GrpcWireClient(String target, TlsMode tls, byte[] serverTrustPem) {
        this.channel = Grpc.newChannelBuilder(target, credentials(tls, serverTrustPem)).build();
    }

    /// Drives the export service for the signal with a minimal valid request.
    ///
    /// @param signal the signal to export
    /// @return the captured response
    public WireResponse export(ConformanceSignal signal) {
        return call(signal, false, 0);
    }

    /// Drives the export service without blocking the calling thread, so an admission test can hold
    /// the first request open while it sends a second.
    ///
    /// @param signal the signal to export
    /// @return a future completing with the captured response
    public CompletableFuture<WireResponse> exportAsync(ConformanceSignal signal) {
        return CompletableFuture.supplyAsync(() -> call(signal, false, 0));
    }

    /// Drives the export service with the request gzip-compressed.
    ///
    /// @param signal the signal to export
    /// @return the captured response
    public WireResponse exportGzip(ConformanceSignal signal) {
        return call(signal, true, 0);
    }

    /// Drives the export service with a request padded past `minBytes`, for the oversized case.
    ///
    /// @param signal the signal to export
    /// @param minBytes the minimum serialized size the request must exceed
    /// @return the captured response
    public WireResponse exportLarge(ConformanceSignal signal, int minBytes) {
        return call(signal, false, minBytes);
    }

    private WireResponse call(ConformanceSignal signal, boolean gzip, int padBytes) {
        try {
            var body = switch (signal) {
                case TRACES -> compress(TraceServiceGrpc.newBlockingStub(channel), gzip)
                        .export(traceRequest(padBytes)).toByteArray();
                case METRICS -> compress(MetricsServiceGrpc.newBlockingStub(channel), gzip)
                        .export(metricsRequest(padBytes)).toByteArray();
                case LOGS -> compress(LogsServiceGrpc.newBlockingStub(channel), gzip)
                        .export(logsRequest(padBytes)).toByteArray();
                case PROFILES -> compress(ProfilesServiceGrpc.newBlockingStub(channel), gzip)
                        .export(profilesRequest(padBytes)).toByteArray();
            };
            return WireResponse.grpc(Status.Code.OK, null, null, body);
        } catch (StatusRuntimeException e) {
            return fromError(e);
        }
    }

    private static <S extends AbstractStub<S>> S compress(S stub, boolean gzip) {
        return gzip ? stub.withCompression("gzip") : stub;
    }

    private static WireResponse fromError(StatusRuntimeException e) {
        var status = e.getStatus();
        Long pushback = null;
        var trailers = e.getTrailers();
        if (trailers != null && trailers.containsKey(PUSHBACK_MS)) {
            pushback = Long.valueOf(trailers.get(PUSHBACK_MS));
        }
        return WireResponse.grpc(status.getCode(), status.getDescription(), pushback, null);
    }

    private static ChannelCredentials credentials(TlsMode tls, byte[] serverTrustPem) {
        try {
            return switch (tls) {
                case NONE -> InsecureChannelCredentials.create();
                case SERVER_AUTH -> TlsChannelCredentials.newBuilder()
                        .trustManager(new ByteArrayInputStream(serverTrustPem))
                        .build();
                case MUTUAL -> TlsChannelCredentials.newBuilder()
                        .trustManager(new ByteArrayInputStream(serverTrustPem))
                        .keyManager(TlsTestMaterial.clientCertStream(), TlsTestMaterial.clientKeyStream())
                        .build();
            };
        } catch (IOException e) {
            throw new UncheckedIOException("failed to build gRPC channel credentials", e);
        }
    }

    private static ExportTraceServiceRequest traceRequest(int padBytes) {
        var builder = ExportTraceServiceRequest.newBuilder();
        if (padBytes > 0) {
            builder.addResourceSpans(ResourceSpans.newBuilder().setSchemaUrl("x".repeat(padBytes)));
        }
        return builder.build();
    }

    private static ExportMetricsServiceRequest metricsRequest(int padBytes) {
        var builder = ExportMetricsServiceRequest.newBuilder();
        if (padBytes > 0) {
            builder.addResourceMetrics(ResourceMetrics.newBuilder().setSchemaUrl("x".repeat(padBytes)));
        }
        return builder.build();
    }

    private static ExportLogsServiceRequest logsRequest(int padBytes) {
        var builder = ExportLogsServiceRequest.newBuilder();
        if (padBytes > 0) {
            builder.addResourceLogs(ResourceLogs.newBuilder().setSchemaUrl("x".repeat(padBytes)));
        }
        return builder.build();
    }

    private static ExportProfilesServiceRequest profilesRequest(int padBytes) {
        var builder = ExportProfilesServiceRequest.newBuilder();
        if (padBytes > 0) {
            builder.addResourceProfiles(ResourceProfiles.newBuilder().setSchemaUrl("x".repeat(padBytes)));
        }
        return builder.build();
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
