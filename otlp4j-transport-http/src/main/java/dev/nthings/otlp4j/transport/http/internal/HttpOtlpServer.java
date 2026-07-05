package dev.nthings.otlp4j.transport.http.internal;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import dev.nthings.otlp4j.transport.common.AdmissionControl;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// OTLP/HTTP implementation of the [OtlpServer] SPI, built on the JDK [HttpServer]/[HttpsServer].
///
/// Serves the four OTLP collector endpoints and routes decoded requests to per-signal
/// [Dispatchers]. [Tls] selects the server: [Tls.Disabled] is plaintext; [Tls.Custom] and
/// [Tls.Inline] supply the certificate and key; [Tls.SystemTrust] and [Tls.SslContext] are rejected
/// as a server has no certificate of its own.
///
/// When no [ServerConfig#serverExecutor()] is configured the server uses a
/// virtual-thread-per-request executor so a handler blocking on its dispatcher's
/// [CompletionStage] never starves the others.
///
/// `HttpExchangeHandlers.handle` joins the dispatcher stage, which is safe only because that
/// default executor is virtual-thread-per-request. A bounded `serverExecutor` composes with
/// admission along a different axis: the pool caps handler concurrency, admission caps decoded
/// in-flight requests/bytes, and a request blocked waiting for a pool thread holds no admission
/// permit yet (the permit is acquired inside the handler). A bounded pool smaller than the offered
/// load therefore queues exchanges at the pool, not at admission.
public final class HttpOtlpServer implements OtlpServer {

    private static final Logger log = LoggerFactory.getLogger(HttpOtlpServer.class);

    /// Runs each blocking stop/drain on a virtual thread.
    private static final Executor SHUTDOWN_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-http-shutdown").start(task);

    private final ServerConfig config;
    private final Dispatchers dispatchers;
    private final GatewayMetrics telemetry;

    private HttpServer server;
    private Executor executor;
    private boolean ownsExecutor;
    private volatile @Nullable AdmissionControl admission;

    /// Full constructor. `telemetry` receives the admission instruments.
    ///
    /// @param config      the transport config
    /// @param dispatchers the per-signal dispatch functions
    /// @param telemetry   the self-observability registry for admission health
    public HttpOtlpServer(ServerConfig config, Dispatchers dispatchers, GatewayMetrics telemetry) {
        this.config = config;
        this.dispatchers = dispatchers;
        this.telemetry = telemetry;
    }

    /// Convenience constructor with no self-observability ([GatewayMetrics#noop]).
    ///
    /// @param config      the transport config
    /// @param dispatchers the per-signal dispatch functions
    public HttpOtlpServer(ServerConfig config, Dispatchers dispatchers) {
        this(config, dispatchers, GatewayMetrics.noop());
    }

    @Override
    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("server already started");
        }
        var http = create(config.tls(), Transports.bindAddress(config.bindHost(), config.port()));
        this.ownsExecutor = config.serverExecutor() == null;
        this.executor = ownsExecutor ? Executors.newVirtualThreadPerTaskExecutor() : config.serverExecutor();
        http.setExecutor(executor);
        var admissionControl = AdmissionControl.of(config, telemetry);
        this.admission = admissionControl;
        HttpExchangeHandlers.register(
                http, dispatchers, config.maxInboundMessageSizeBytes(), admissionControl);
        http.start();
        this.server = http;
        log.info("OTLP/HTTP server listening on port {}", port());
    }

    private static HttpServer create(Tls tls, InetSocketAddress address) throws IOException {
        return switch (tls) {
            case Tls.Disabled _ -> HttpServer.create(address, 0);
            case Tls.Custom(var certFile, var keyFile, var _) -> {
                if (certFile == null || keyFile == null) {
                    throw new IllegalArgumentException(
                            "Tls.Custom for a server requires both a certificate and a key");
                }
                var https = HttpsServer.create(address, 0);
                https.setHttpsConfigurator(new HttpsConfigurator(PemSsl.serverContext(certFile, keyFile)));
                yield https;
            }
            case Tls.Inline(var cert, var key, var _) -> {
                if (cert == null || key == null) {
                    throw new IllegalArgumentException(
                            "in-memory server TLS requires both a certificate and a key");
                }
                var https = HttpsServer.create(address, 0);
                https.setHttpsConfigurator(new HttpsConfigurator(PemSsl.serverContext(cert, key)));
                yield https;
            }
            case Tls.SystemTrust _ -> throw new IllegalArgumentException(
                    "Tls.SystemTrust is not valid for a server; use Tls.Custom with a certificate and key");
            case Tls.SslContext _ -> throw new IllegalArgumentException(
                    "Tls.SslContext is not valid for a server; use Tls.Custom with a certificate and key");
        };
    }

    @Override
    public synchronized int port() {
        return server == null ? 0 : server.getAddress().getPort();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        // HttpServer.stop(delay) closes the listener, then waits up to `delay` seconds for
        // in-flight exchanges to finish - a graceful drain bounded by the caller's deadline.
        return stop(clampSeconds(timeout));
    }

    @Override
    public CompletionStage<Void> shutdownNow() {
        return stop(0);
    }

    private CompletionStage<Void> stop(int delaySeconds) {
        HttpServer current;
        synchronized (this) {
            current = server;
        }
        if (current == null) {
            return CompletableFuture.completedFuture(null);
        }
        closeAdmission();
        return CompletableFuture.runAsync(() -> {
            current.stop(delaySeconds);
            if (ownsExecutor && executor instanceof ExecutorService es) {
                es.shutdown();
            }
        }, SHUTDOWN_EXECUTOR);
    }

    /// Closes the admission control (idempotent) so its in-flight gauge registrations are released on
    /// shutdown; both [#shutdown] and [#shutdownNow] route through [#stop]. A null admission (server
    /// never started) is a no-op.
    private void closeAdmission() {
        var current = admission;
        if (current != null) {
            current.close();
        }
    }

    /// Clamps a drain timeout to the `int` seconds [HttpServer#stop(int)] expects,
    /// guarding against overflow on a very large [Duration].
    private static int clampSeconds(Duration timeout) {
        return (int) Math.max(0, Math.min(timeout.toSeconds(), Integer.MAX_VALUE));
    }
}
