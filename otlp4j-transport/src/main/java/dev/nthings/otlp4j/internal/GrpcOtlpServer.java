package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Plaintext OTLP/gRPC implementation of the [OtlpServer] SPI.
///
/// Serves the four OTLP collector services and routes decoded requests to the per-signal
/// dispatchers passed in by [GrpcOtlpServerProvider].
final class GrpcOtlpServer implements OtlpServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcOtlpServer.class);

    private final ServerTransportConfig config;
    private final OtlpServerProvider.Dispatchers dispatchers;

    private Server server;

    GrpcOtlpServer(ServerTransportConfig config, OtlpServerProvider.Dispatchers dispatchers) {
        this.config = config;
        this.dispatchers = dispatchers;
    }

    @Override
    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("server already started");
        }
        // TLS configuration on the SPI is honoured for `Disabled`; other variants fall back to
        // plaintext in this v1 transport. Future TLS implementation lives here without an SPI break.
        var serverBuilder = Grpc.newServerBuilderForPort(config.port(), InsecureServerCredentials.create());
        GrpcServiceAdapters.create(dispatchers).forEach(serverBuilder::addService);
        server = serverBuilder.build().start();
        log.info("OTLP/gRPC server listening on port {}", server.getPort());
    }

    @Override
    public synchronized int port() {
        return server == null ? 0 : server.getPort();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        Server current;
        synchronized (this) {
            current = server;
        }
        if (current == null) {
            return CompletableFuture.completedFuture(null);
        }
        log.debug("initiating graceful shutdown of OTLP/gRPC server");
        current.shutdown();
        return waitForTermination(current, timeout);
    }

    @Override
    public CompletionStage<Void> shutdownNow() {
        Server current;
        synchronized (this) {
            current = server;
        }
        if (current == null) {
            return CompletableFuture.completedFuture(null);
        }
        log.debug("forcing shutdown of OTLP/gRPC server");
        current.shutdownNow();
        return waitForTermination(current, Duration.ofSeconds(5));
    }

    private static CompletionStage<Void> waitForTermination(Server server, Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!server.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    throw new IllegalStateException("OTLP/gRPC server did not terminate within " + timeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
    }
}
