package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests that the OTLP/HTTP transport honours the full client/server config surface: signal paths,
/// content type, headers, gzip, TLS, and the IPv6/retry edge cases.
///
/// Request-shape tests point an exporter at a bare capturing [HttpServer]; TLS is exercised
/// end-to-end through the real receiver and exporter.
@Timeout(30)
@DisplayName("OTLP/HTTP transport config")
class HttpTransportConfigTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<OtlpHttpReceiver> receivers = new ArrayList<>();
    private HttpServer rawServer;

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception _) { /* keep tearing down */ }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) { /* keep tearing down */ }
        }
        if (rawServer != null) {
            rawServer.stop(0);
        }
    }

    @DisplayName("POSTs each signal to its standard path with a protobuf content type")
    @Test
    void postsToStandardSignalPaths() {
        var captured = new ArrayList<Captured>();
        var port = startRawServer(captured);
        var exporter = exporter(ClientConfig.builder().endpoint("localhost", port).build());

        exporter.traces().consume(TransportFixtures.richTraceData()).toCompletableFuture().join();
        exporter.metrics().consume(TransportFixtures.richMetricsData()).toCompletableFuture().join();
        exporter.logs().consume(TransportFixtures.richLogsData()).toCompletableFuture().join();
        exporter.profiles().consume(TransportFixtures.profilesData()).toCompletableFuture().join();

        assertThat(captured).extracting(Captured::path)
                .containsExactlyInAnyOrder("/v1/traces", "/v1/metrics", "/v1/logs", "/v1development/profiles");
        assertThat(captured).allMatch(c -> c.method().equals("POST"));
        assertThat(captured).allMatch(c -> "application/x-protobuf".equals(c.contentType()));
        assertThat(captured).allMatch(c -> c.bodyLength() > 0);
    }

    @DisplayName("Attaches configured headers to the request")
    @Test
    void attachesConfiguredHeaders() {
        var captured = new ArrayList<Captured>();
        var port = startRawServer(captured);
        var exporter = exporter(ClientConfig.builder()
                .endpoint("localhost", port)
                .header("x-otlp-api-key", "secret")
                .build());

        exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(captured).singleElement().satisfies(c ->
                assertThat(c.apiKey()).isEqualTo("secret"));
    }

    @DisplayName("Compresses the body and sets Content-Encoding when gzip is configured")
    @Test
    void compressesWithGzipWhenConfigured() {
        var captured = new ArrayList<Captured>();
        var port = startRawServer(captured);
        var exporter = exporter(ClientConfig.builder()
                .endpoint("localhost", port)
                .compression(Compression.GZIP)
                .build());

        exporter.traces().consume(TransportFixtures.richTraceData()).toCompletableFuture().join();

        assertThat(captured).singleElement().satisfies(c -> {
            assertThat(c.contentEncoding()).isEqualTo("gzip");
            assertThat(c.bodyLength()).isPositive(); // decoded successfully from gzip
        });
    }

    @DisplayName("Round-trips TraceData over custom TLS")
    @Test
    void roundTripsOverTls() {
        var received = new java.util.concurrent.atomic.AtomicReference<TraceData>();
        var receiver = startReceiver(serverTls(), OtlpHttpReceiver.builder().onTraces(t -> {
            received.set(t);
            return ConsumeResult.acceptedStage();
        }));
        var exporter = exporter(ClientConfig.builder()
                .endpoint("localhost", receiver.port())
                .tls(Tls.trust(resource("/tls/server.crt")))
                .timeout(Duration.ofSeconds(5))
                .build());

        var sent = traces();
        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Plaintext client fails against a TLS server")
    @Test
    void plaintextClientFailsAgainstTlsServer() {
        var receiver = startReceiver(serverTls(),
                OtlpHttpReceiver.builder().onTraces(t -> ConsumeResult.acceptedStage()));
        var exporter = exporter(ClientConfig.builder()
                .endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(3))
                .build());

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("Builds a client exporter with SystemTrust TLS")
    @Test
    void buildsClientWithSystemTrustTls() {
        try (var exporter = OtlpHttpExporter.builder()
                .transport(ClientConfig.builder()
                        .endpoint("localhost", 4318)
                        .tls(Tls.systemTrust())
                        .build())
                .build()) {
            assertThat(exporter).isNotNull();
        }
    }

    @DisplayName("Builds a client exporter for an IPv6 host")
    @Test
    void buildsClientForIpv6Host() {
        try (var exporter = OtlpHttpExporter.builder().host("::1").port(4318).build()) {
            assertThat(exporter).isNotNull();
        }
    }

    @DisplayName("Rejects SystemTrust TLS for a server")
    @Test
    void rejectsSystemTrustForServer() {
        var receiver = OtlpHttpReceiver.builder()
                .transport(ServerConfig.builder().port(0).tls(Tls.systemTrust()).build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SystemTrust");
    }

    @DisplayName("A server Tls.Custom requires both a certificate and a key")
    @Test
    void serverCustomTlsRequiresCertAndKey() {
        var receiver = OtlpHttpReceiver.builder()
                .transport(ServerConfig.builder()
                        .port(0)
                        .tls(Tls.custom(resource("/tls/server.crt"), null, null))
                        .build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
    }

    @DisplayName("Half-specified client mutual-TLS material is rejected")
    @Test
    void halfSpecifiedClientMtlsIsRejected() {
        var config = ClientConfig.builder()
                .endpoint("localhost", 4318)
                .tls(Tls.custom(resource("/tls/server.crt"), null, null))
                .build();

        assertThatThrownBy(() -> exporter(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutual-TLS");
    }

    @DisplayName("A specific bindHost binds that interface only and starts")
    @Test
    void specificBindHostBindsThatInterface() {
        var receiver = OtlpHttpReceiver.builder()
                .transport(ServerConfig.builder().bindHost("127.0.0.1").port(0).build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        receiver.start();
        assertThat(receiver.port()).isPositive();
    }

    @DisplayName("Retries an IO failure within the budget, then surfaces it")
    @Test
    void retriesIoFailureThenSurfacesIt() {
        // Grab a port, then free it, so connections are refused.
        var probe = OtlpHttpReceiver.on(0).start();
        var deadPort = probe.port();
        probe.shutdownNow().toCompletableFuture().join();

        var exporter = OtlpHttpExporter.builder()
                .endpoint("localhost", deadPort)
                .retry(dev.nthings.otlp4j.config.RetryPolicy.exponential(2, Duration.ofMillis(1), Duration.ofMillis(5)))
                .timeout(Duration.ofSeconds(2))
                .build();
        closeables.add(exporter);

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    private OtlpHttpReceiver startReceiver(ServerConfig config, OtlpHttpReceiver.Builder builder) {
        var receiver = builder.transport(config).build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpHttpExporter exporter(ClientConfig config) {
        var exporter = OtlpHttpExporter.builder().transport(config).build();
        closeables.add(exporter);
        return exporter;
    }

    /// Starts a bare HTTP server that records each exchange and acks with an empty 200.
    private int startRawServer(List<Captured> captured) {
        try {
            rawServer = HttpServer.create(new InetSocketAddress(0), 0);
            rawServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            rawServer.createContext("/", exchange -> {
                var headers = exchange.getRequestHeaders();
                var encoding = headers.getFirst("Content-Encoding");
                InputStream in = exchange.getRequestBody();
                if ("gzip".equalsIgnoreCase(encoding)) {
                    in = new GZIPInputStream(in);
                }
                var body = in.readAllBytes();
                captured.add(new Captured(
                        exchange.getRequestMethod(),
                        exchange.getRequestURI().getPath(),
                        headers.getFirst("Content-Type"),
                        encoding,
                        headers.getFirst("x-otlp-api-key"),
                        body.length));
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            });
            rawServer.start();
            return rawServer.getAddress().getPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ServerConfig serverTls() {
        return ServerConfig.builder()
                .port(0)
                .tls(Tls.custom(resource("/tls/server.crt"), resource("/tls/server.key"), null))
                .build();
    }

    private static TraceData traces() {
        return Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER));
    }

    private static Path resource(String name) {
        try {
            return Path.of(HttpTransportConfigTest.class.getResource(name).toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private record Captured(
            String method,
            String path,
            String contentType,
            String contentEncoding,
            String apiKey,
            int bodyLength) {}
}
