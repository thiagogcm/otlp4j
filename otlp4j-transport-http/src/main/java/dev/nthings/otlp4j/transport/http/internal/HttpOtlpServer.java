package dev.nthings.otlp4j.transport.http.internal;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// OTLP/HTTP implementation of the [OtlpServer] SPI, built on the JDK [HttpServer]/[HttpsServer].
///
/// Serves the four OTLP collector endpoints (`/v1/traces`, `/v1/metrics`, `/v1/logs`,
/// `/v1development/profiles`) and routes decoded requests to the per-signal [Dispatchers] the
/// receiver supplies. [Tls] selects the server: [Tls.Disabled] is plaintext and [Tls.Custom]
/// supplies the certificate and key (via an [HttpsServer]); [Tls.SystemTrust] is rejected, as a
/// server has no certificate of its own.
///
/// When no [ServerConfig#serverExecutor()] is configured the server uses a
/// virtual-thread-per-request executor, so a handler blocking on its dispatcher's [CompletionStage]
/// never starves the others.
public final class HttpOtlpServer implements OtlpServer {

    private static final Logger log = LoggerFactory.getLogger(HttpOtlpServer.class);

    /// Runs each blocking stop/drain on a virtual thread, not a common-pool worker.
    private static final Executor SHUTDOWN_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-http-shutdown").start(task);

    private final ServerConfig config;
    private final Dispatchers dispatchers;

    private HttpServer server;
    private Executor executor;
    private boolean ownsExecutor;

    public HttpOtlpServer(ServerConfig config, Dispatchers dispatchers) {
        this.config = config;
        this.dispatchers = dispatchers;
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
        HttpExchangeHandlers.register(http, dispatchers, config.maxInboundMessageSizeBytes());
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
            case Tls.SystemTrust _ -> throw new IllegalArgumentException(
                    "Tls.SystemTrust is not valid for a server; use Tls.Custom with a certificate and key");
        };
    }

    @Override
    public synchronized int port() {
        return server == null ? 0 : server.getAddress().getPort();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        // HttpServer.stop(delay) closes the listener, then waits up to `delay` seconds for in-flight
        // exchanges to finish before closing them — a graceful drain bounded by the caller's deadline.
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
        return CompletableFuture.runAsync(() -> {
            current.stop(delaySeconds);
            if (ownsExecutor && executor instanceof ExecutorService es) {
                es.shutdown();
            }
        }, SHUTDOWN_EXECUTOR);
    }

    /// Clamps a drain timeout to the non-negative `int` seconds [HttpServer#stop(int)] expects,
    /// guarding against overflow on a very large [Duration].
    private static int clampSeconds(Duration timeout) {
        return (int) Math.max(0, Math.min(timeout.toSeconds(), Integer.MAX_VALUE));
    }
}
