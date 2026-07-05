package dev.nthings.otlp4j.transport.grpc.internal;

import dev.nthings.otlp4j.transport.common.AdmissionControl;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.NettyServerBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// OTLP/gRPC implementation of the [OtlpServer] SPI.
///
/// Serves the four OTLP collector services and routes decoded requests to the
/// per-signal [Dispatchers]. [Tls] selects the server credentials: [Tls.Disabled]
/// is plaintext; [Tls.Custom] and [Tls.Inline] supply the certificate and key;
/// [Tls.SystemTrust] and [Tls.SslContext] are rejected as a server has no certificate of its own.
///
/// Built on Netty's [NettyServerBuilder] so it can bind a specific interface and
/// apply limits from [ServerConfig] (decoded-request cap, handshake deadline, optional bounded
/// executor) plus the gRPC-only per-connection concurrency cap passed to the constructor.
public final class GrpcOtlpServer implements OtlpServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcOtlpServer.class);

    /// Runs each blocking termination wait on a virtual thread.
    private static final Executor SHUTDOWN_EXECUTOR = task -> Thread.ofVirtual().name("otlp-grpc-shutdown").start(task);

    private final ServerConfig config;
    private final Dispatchers dispatchers;
    private final GatewayMetrics telemetry;
    private final int maxConcurrentCallsPerConnection;

    private Server server;
    private volatile @Nullable AdmissionControl admission;

    /// Full constructor. `telemetry` receives the admission instruments; `maxConcurrentCallsPerConnection`
    /// is a gRPC-only hardening knob (`0` keeps gRPC's unlimited default); HTTP has no equivalent, which
    /// is why it is a transport-local ctor arg rather than a shared [ServerConfig] field.
    ///
    /// @param config                          the transport config
    /// @param dispatchers                     the per-signal dispatch functions
    /// @param telemetry                       the self-observability registry for admission health
    /// @param maxConcurrentCallsPerConnection the per-connection concurrency cap, or `0` for unlimited
    public GrpcOtlpServer(ServerConfig config, Dispatchers dispatchers, GatewayMetrics telemetry,
            int maxConcurrentCallsPerConnection) {
        this.config = config;
        this.dispatchers = dispatchers;
        this.telemetry = telemetry;
        this.maxConcurrentCallsPerConnection = maxConcurrentCallsPerConnection;
    }

    /// Convenience constructor with the given concurrency cap and no self-observability
    /// ([GatewayMetrics#noop]).
    ///
    /// @param config                          the transport config
    /// @param dispatchers                     the per-signal dispatch functions
    /// @param maxConcurrentCallsPerConnection the per-connection concurrency cap, or `0` for unlimited
    public GrpcOtlpServer(ServerConfig config, Dispatchers dispatchers, int maxConcurrentCallsPerConnection) {
        this(config, dispatchers, GatewayMetrics.noop(), maxConcurrentCallsPerConnection);
    }

    /// Convenience constructor with no per-connection concurrency cap (`0`, gRPC's unlimited default)
    /// and no self-observability ([GatewayMetrics#noop]).
    ///
    /// @param config      the transport config
    /// @param dispatchers the per-signal dispatch functions
    public GrpcOtlpServer(ServerConfig config, Dispatchers dispatchers) {
        this(config, dispatchers, GatewayMetrics.noop(), 0);
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
        if (maxConcurrentCallsPerConnection > 0) {
            builder.maxConcurrentCallsPerConnection(maxConcurrentCallsPerConnection);
        }
        if (config.serverExecutor() != null) {
            builder.executor(config.serverExecutor());
        }
        var admissionControl = AdmissionControl.of(config, telemetry);
        this.admission = admissionControl;
        GrpcServiceAdapters.create(dispatchers, admissionControl).forEach(builder::addService);
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
        closeAdmission();
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
        closeAdmission();
        current.shutdownNow();
        return waitForTermination(current, Duration.ofSeconds(5));
    }

    /// Closes the admission control (idempotent) so its in-flight gauge registrations are released
    /// on shutdown; a null admission (server never started) is a no-op.
    private void closeAdmission() {
        var current = admission;
        if (current != null) {
            current.close();
        }
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
        }, SHUTDOWN_EXECUTOR);
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
            case Tls.Inline(var cert, var key, var _) -> {
                if (cert == null || key == null) {
                    throw new IllegalArgumentException(
                            "in-memory server TLS requires both a certificate and a key");
                }
                try {
                    yield TlsServerCredentials.newBuilder()
                            .keyManager(new ByteArrayInputStream(cert), new ByteArrayInputStream(key))
                            .build();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to load server TLS material", e);
                }
            }
            case Tls.SystemTrust() -> throw new IllegalArgumentException(
                    "Tls.SystemTrust is not valid for a server; use Tls.Custom with a certificate and key");
            case Tls.SslContext(var _, var _) -> throw new IllegalArgumentException(
                    "Tls.SslContext is not valid for a server; use Tls.Custom with a certificate and key");
        };
    }
}
