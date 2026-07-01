package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.OtlpExporter;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
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
    private final List<Receiver> receivers = new ArrayList<>();
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
        var exporter = exporter(ClientConfig.builder().setEndpoint("localhost", port).build());

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
                .setEndpoint("localhost", port)
                .addHeader("x-otlp-api-key", "secret")
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
                .setEndpoint("localhost", port)
                .setCompression(Compression.GZIP)
                .build());

        exporter.traces().consume(TransportFixtures.richTraceData()).toCompletableFuture().join();

        assertThat(captured).singleElement().satisfies(c -> {
            assertThat(c.contentEncoding()).isEqualTo("gzip");
            assertThat(c.bodyLength()).isPositive(); // decoded successfully from gzip
        });
    }

    @DisplayName("Round-trips TracesData over custom TLS")
    @Test
    void roundTripsOverTls() {
        var received = new java.util.concurrent.atomic.AtomicReference<TracesData>();
        var receiver = startReceiver(serverTls(), OtlpHttpReceiver.builder().onTraces(t -> {
            received.set(t);
            return ConsumeResult.acceptedStage();
        }));
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(resource("/tls/server.crt")))
                .setTimeout(Duration.ofSeconds(5))
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
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(3))
                .build());

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("Builds a client exporter with SystemTrust TLS")
    @Test
    void buildsClientWithSystemTrustTls() {
        try (var exporter = OtlpHttpExporter.builder()
                .setConfig(ClientConfig.builder()
                        .setEndpoint("localhost", 4318)
                        .setTls(Tls.systemTrust())
                        .build())
                .build()) {
            assertThat(exporter).isNotNull();
        }
    }

    @DisplayName("Builds a client exporter for an IPv6 host")
    @Test
    void buildsClientForIpv6Host() {
        try (var exporter = OtlpHttpExporter.builder().setEndpoint("::1", 4318).build()) {
            assertThat(exporter).isNotNull();
        }
    }

    @DisplayName("Rejects SystemTrust TLS for a server")
    @Test
    void rejectsSystemTrustForServer() {
        var receiver = OtlpHttpReceiver.builder()
                .setConfig(ServerConfig.builder().setPort(0).setTls(Tls.systemTrust()).build())
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
                .setConfig(ServerConfig.builder()
                        .setPort(0)
                        .setTls(Tls.custom(resource("/tls/server.crt"), null, null))
                        .build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and a key");
    }

    @DisplayName("Rejects SslContext TLS for a server")
    @Test
    void rejectsSslContextForServer() throws Exception {
        var trustManager = trustManagerFor(resource("/tls/server.crt"));
        var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] {trustManager}, null);
        var receiver = OtlpHttpReceiver.builder()
                .setConfig(ServerConfig.builder()
                        .setPort(0)
                        .setTls(Tls.sslContext(sslContext, trustManager))
                        .build())
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        receivers.add(receiver);

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SslContext");
    }

    @DisplayName("A server in-memory TLS without a key is rejected")
    @Test
    void serverInMemoryTrustOnlyRejected() throws Exception {
        var receiver = OtlpHttpReceiver.builder()
                .setConfig(ServerConfig.builder()
                        .setPort(0)
                        .setTls(Tls.trust(bytes("/tls/server.crt")))
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
                .setEndpoint("localhost", 4318)
                .setTls(Tls.custom(resource("/tls/server.crt"), null, null))
                .build();

        assertThatThrownBy(() -> exporter(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutual-TLS");
    }

    @DisplayName("A specific bindHost binds that interface only and starts")
    @Test
    void specificBindHostBindsThatInterface() {
        var receiver = OtlpHttpReceiver.builder()
                .setConfig(ServerConfig.builder().setBindHost("127.0.0.1").setPort(0).build())
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
        var probe = OtlpHttpReceiver.builder().ephemeralPort().build().start();
        var deadPort = probe.port();
        probe.shutdownNow().toCompletableFuture().join();

        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", deadPort)
                .setRetryPolicy(dev.nthings.otlp4j.config.RetryPolicy.builder().setMaxAttempts(2).setInitialBackoff(Duration.ofMillis(1)).setMaxBackoff(Duration.ofMillis(5)).build())
                .setTimeout(Duration.ofSeconds(2))
                .build();
        closeables.add(exporter);

        assertThatThrownBy(() -> exporter.traces().consume(traces()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    @DisplayName("Round-trips TracesData over in-memory (byte[]) TLS")
    @Test
    void roundTripsOverInMemoryTls() throws Exception {
        var received = new AtomicReference<TracesData>();
        var serverConfig = ServerConfig.builder()
                .setPort(0)
                .setTls(Tls.custom(bytes("/tls/server.crt"), bytes("/tls/server.key"), null))
                .build();
        var receiver = startReceiver(serverConfig, OtlpHttpReceiver.builder().onTraces(t -> {
            received.set(t);
            return ConsumeResult.acceptedStage();
        }));
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.trust(bytes("/tls/server.crt")))
                .setTimeout(Duration.ofSeconds(5))
                .build());

        var sent = traces();
        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips over TLS using a caller-built SSLContext (used verbatim on HTTP)")
    @Test
    void roundTripsOverSslContext() throws Exception {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(serverTls(), OtlpHttpReceiver.builder().onTraces(t -> {
            received.set(t);
            return ConsumeResult.acceptedStage();
        }));
        var trustManager = trustManagerFor(resource("/tls/server.crt"));
        var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] {trustManager}, null);
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTls(Tls.sslContext(sslContext, trustManager))
                .setTimeout(Duration.ofSeconds(5))
                .build());

        var sent = traces();
        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Re-evaluates the header supplier on every request")
    @Test
    void reevaluatesHeaderSupplierPerRequest() {
        var captured = new ArrayList<Captured>();
        var port = startRawServer(captured);
        var counter = new AtomicInteger();
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", port)
                .setHeaders(() -> Map.of("x-otlp-api-key", "token-" + counter.incrementAndGet()))
                .build());

        exporter.traces().consume(traces()).toCompletableFuture().join();
        exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(captured).extracting(Captured::apiKey).containsExactly("token-1", "token-2");
    }

    @DisplayName("Honours a configured connect timeout on a normal round-trip")
    @Test
    void appliesConnectTimeout() {
        var captured = new ArrayList<Captured>();
        var port = startRawServer(captured);
        var exporter = exporter(ClientConfig.builder()
                .setEndpoint("localhost", port)
                .setConnectTimeout(Duration.ofSeconds(3))
                .build());

        var result = exporter.traces().consume(traces()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(captured).hasSize(1);
    }

    private Receiver startReceiver(ServerConfig config, OtlpHttpReceiver.Builder builder) {
        var receiver = builder.setConfig(config).build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpExporter exporter(ClientConfig config) {
        var exporter = OtlpHttpExporter.builder().setConfig(config).build();
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
                .setPort(0)
                .setTls(Tls.custom(resource("/tls/server.crt"), resource("/tls/server.key"), null))
                .build();
    }

    private static TracesData traces() {
        return Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER));
    }

    private static Path resource(String name) {
        try {
            return Path.of(HttpTransportConfigTest.class.getResource(name).toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] bytes(String name) throws Exception {
        return Files.readAllBytes(resource(name));
    }

    /// Builds an X509 trust manager trusting the certificate(s) in `certPem`, for the SSLContext TLS
    /// variant.
    private static X509TrustManager trustManagerFor(Path certPem) throws Exception {
        var factory = CertificateFactory.getInstance("X.509");
        var ks = KeyStore.getInstance("PKCS12");
        ks.load(null, new char[0]);
        try (InputStream in = Files.newInputStream(certPem)) {
            var i = 0;
            for (var cert : factory.generateCertificates(in)) {
                ks.setCertificateEntry("ca-" + i++, (X509Certificate) cert);
            }
        }
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        for (var tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager x) {
                return x;
            }
        }
        throw new IllegalStateException("no X509TrustManager available");
    }

    private record Captured(
            String method,
            String path,
            String contentType,
            String contentEncoding,
            String apiKey,
            int bodyLength) {}
}
