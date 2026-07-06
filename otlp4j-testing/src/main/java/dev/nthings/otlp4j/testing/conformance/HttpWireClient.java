package dev.nthings.otlp4j.testing.conformance;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.GZIPOutputStream;

/// A black-box OTLP/HTTP driver built on the JDK [HttpClient].
///
/// It posts a built export request to the correct per-signal path, can override the content type,
/// gzip the body, send an arbitrary byte body for the malformed and oversized cases, and issue a
/// wrong-method GET. When the scenario uses TLS it trusts the server CA the gateway handed back
/// and, for the mutual case, presents the checked-in client key material. It never touches
/// otlp4j-codec, keeping the suite genuinely black-box at the wire boundary.
public final class HttpWireClient implements AutoCloseable {

    /// The OTLP/HTTP binary-protobuf media type.
    public static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    private final URI baseUri;
    private final HttpClient client;

    /// Creates a client against the gateway's HTTP base URI with the given TLS posture.
    ///
    /// @param baseUri the gateway base URI, for example `https://127.0.0.1:53412`
    /// @param tls the TLS posture to take
    /// @param serverTrustPem the PEM to trust as the server CA, or null for a plaintext scenario
    public HttpWireClient(URI baseUri, TlsMode tls, byte[] serverTrustPem) {
        this.baseUri = baseUri;
        var builder = HttpClient.newBuilder();
        if (tls != TlsMode.NONE) {
            builder.sslContext(TlsTestMaterial.clientSslContext(serverTrustPem, tls == TlsMode.MUTUAL));
        }
        this.client = builder.build();
    }

    /// Posts a minimal valid export request for the signal with the protobuf content type.
    ///
    /// @param signal the signal to post to
    /// @return the captured response
    public WireResponse export(ConformanceSignal signal) {
        return post(signal, emptyRequest(signal), PROTOBUF_CONTENT_TYPE, false);
    }

    /// Posts a minimal valid export request without blocking, so an admission test can hold the
    /// first request open while it sends a second.
    ///
    /// @param signal the signal to post to
    /// @return a future completing with the captured response
    public CompletableFuture<WireResponse> exportAsync(ConformanceSignal signal) {
        return sendAsync(request(signal, emptyRequest(signal), PROTOBUF_CONTENT_TYPE, false));
    }

    /// Posts the request gzip-compressed with `Content-Encoding: gzip`.
    ///
    /// @param signal the signal to post to
    /// @return the captured response
    public WireResponse exportGzip(ConformanceSignal signal) {
        return post(signal, emptyRequest(signal), PROTOBUF_CONTENT_TYPE, true);
    }

    /// Posts the request with an explicit content type, or none when `contentType` is null.
    ///
    /// @param signal the signal to post to
    /// @param contentType the `Content-Type` to send, or null to omit the header
    /// @return the captured response
    public WireResponse exportWithContentType(ConformanceSignal signal, String contentType) {
        return post(signal, emptyRequest(signal), contentType, false);
    }

    /// Posts an arbitrary byte body with the protobuf content type, for the malformed case.
    ///
    /// @param signal the signal to post to
    /// @param body the raw body bytes
    /// @return the captured response
    public WireResponse exportRaw(ConformanceSignal signal, byte[] body) {
        return post(signal, body, PROTOBUF_CONTENT_TYPE, false);
    }

    /// Posts a valid request padded past `minBytes`, for the oversized case.
    ///
    /// @param signal the signal to post to
    /// @param minBytes the minimum serialized size the body must exceed
    /// @return the captured response
    public WireResponse exportLarge(ConformanceSignal signal, int minBytes) {
        return post(signal, largeRequest(signal, minBytes), PROTOBUF_CONTENT_TYPE, false);
    }

    /// Issues a `GET` to the signal path, for the wrong-method case.
    ///
    /// @param signal the signal whose path to GET
    /// @return the captured response
    public WireResponse get(ConformanceSignal signal) {
        var request = HttpRequest.newBuilder(baseUri.resolve(signal.httpPath())).GET().build();
        return send(request);
    }

    private WireResponse post(ConformanceSignal signal, byte[] body, String contentType, boolean gzip) {
        return send(request(signal, body, contentType, gzip));
    }

    private HttpRequest request(ConformanceSignal signal, byte[] body, String contentType, boolean gzip) {
        var payload = gzip ? gzip(body) : body;
        var builder = HttpRequest.newBuilder(baseUri.resolve(signal.httpPath()))
                .POST(BodyPublishers.ofByteArray(payload));
        if (contentType != null) {
            builder.header("Content-Type", contentType);
        }
        if (gzip) {
            builder.header("Content-Encoding", "gzip");
        }
        return builder.build();
    }

    private WireResponse send(HttpRequest request) {
        try {
            return capture(client.send(request, BodyHandlers.ofByteArray()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while sending HTTP request", e);
        }
    }

    private CompletableFuture<WireResponse> sendAsync(HttpRequest request) {
        return client.sendAsync(request, BodyHandlers.ofByteArray())
                .handle((response, error) -> {
                    if (error != null) {
                        throw new CompletionException(error);
                    }
                    return capture(response);
                });
    }

    private static WireResponse capture(HttpResponse<byte[]> response) {
        var retryAfter = response.headers().firstValue("Retry-After").orElse(null);
        return WireResponse.http(response.statusCode(), retryAfter, response.body());
    }

    private static byte[] gzip(byte[] body) {
        var out = new ByteArrayOutputStream();
        try (var gz = new GZIPOutputStream(out)) {
            gz.write(body);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static byte[] emptyRequest(ConformanceSignal signal) {
        return switch (signal) {
            case TRACES -> ExportTraceServiceRequest.getDefaultInstance().toByteArray();
            case METRICS -> ExportMetricsServiceRequest.getDefaultInstance().toByteArray();
            case LOGS -> ExportLogsServiceRequest.getDefaultInstance().toByteArray();
            case PROFILES -> ExportProfilesServiceRequest.getDefaultInstance().toByteArray();
        };
    }

    private static byte[] largeRequest(ConformanceSignal signal, int minBytes) {
        var pad = "x".repeat(minBytes);
        return switch (signal) {
            case TRACES -> ExportTraceServiceRequest.newBuilder()
                    .addResourceSpans(ResourceSpans.newBuilder().setSchemaUrl(pad))
                    .build().toByteArray();
            case METRICS -> ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(ResourceMetrics.newBuilder().setSchemaUrl(pad))
                    .build().toByteArray();
            case LOGS -> ExportLogsServiceRequest.newBuilder()
                    .addResourceLogs(ResourceLogs.newBuilder().setSchemaUrl(pad))
                    .build().toByteArray();
            case PROFILES -> ExportProfilesServiceRequest.newBuilder()
                    .addResourceProfiles(ResourceProfiles.newBuilder().setSchemaUrl(pad))
                    .build().toByteArray();
        };
    }

    @Override
    public void close() {
        client.close();
    }
}
