package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import dev.nthings.otlp4j.spi.Tls;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// OTLP/gRPC implementation of the [OtlpServer] SPI.
///
/// Serves the four OTLP collector services and routes decoded requests to the per-signal
/// dispatchers passed in by [GrpcOtlpServerProvider]. [Tls] selects the server credentials:
/// [Tls.Disabled] is plaintext and [Tls.Custom] supplies the server certificate and key.
///
/// Built on Netty's [NettyServerBuilder] so it can bind a specific interface (e.g. loopback-only)
/// and apply the production-hardening limits from [ServerTransportConfig]: a decoded-request cap,
/// an optional per-connection concurrency cap, a handshake deadline, and an optional bounded
/// executor.
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
        var builder = NettyServerBuilder.forAddress(
                        Transports.bindAddress(config.bindHost(), config.port()), serverCredentials(config.tls()))
                .maxInboundMessageSize(config.maxInboundMessageSizeBytes())
                .handshakeTimeout(config.handshakeTimeout().toNanos(), TimeUnit.NANOSECONDS);
        if (config.maxConcurrentCallsPerConnection() > 0) {
            builder.maxConcurrentCallsPerConnection(config.maxConcurrentCallsPerConnection());
        }
        if (config.serverExecutor() != null) {
            builder.executor(config.serverExecutor());
        }
        GrpcServiceAdapters.create(dispatchers).forEach(builder::addService);
        server = builder.build().start();
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

    private static ServerCredentials serverCredentials(Tls tls) {
        return switch (tls) {
            case Tls.Disabled() -> InsecureServerCredentials.create();
            case Tls.Custom(var certFile, var keyFile, var _) -> {
                if (certFile == null || keyFile == null) {
                    throw new IllegalArgumentException(
                            "Tls.Custom for a server requires both a certificate and a key");
                }
                try {
                    yield TlsServerCredentials.newBuilder()
                            .keyManager(certFile.toFile(), keyFile.toFile())
                            .build();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to load server TLS material", e);
                }
            }
            case Tls.SystemTrust() -> throw new IllegalArgumentException(
                    "Tls.SystemTrust is not valid for a server; use Tls.Custom with a certificate and key");
        };
    }
}
