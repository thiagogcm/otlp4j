package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.spi.OtlpServer;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Plaintext OTLP/gRPC implementation of the [OtlpServer] SPI.
///
/// Serves the collector services and feeds decoded requests to the configured [TelemetryConsumer].
final class GrpcOtlpServer implements OtlpServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcOtlpServer.class);

    private final TelemetryConsumer consumer;

    private Server server;

    GrpcOtlpServer(TelemetryConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public synchronized void start(int port) throws IOException {
        if (server != null) {
            throw new IllegalStateException("server already started");
        }
        var serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
        GrpcServiceAdapters.create(consumer).forEach(serverBuilder::addService);
        server = serverBuilder.build().start();
        log.info("OTLP/gRPC server listening on port {}", server.getPort());
    }

    @Override
    public synchronized int port() {
        requireStarted();
        return server.getPort();
    }

    @Override
    public synchronized void shutdown() {
        if (server != null) {
            log.debug("initiating graceful shutdown of OTLP/gRPC server");
            server.shutdown();
        }
    }

    @Override
    public synchronized void shutdownNow() {
        if (server != null) {
            log.debug("forcing shutdown of OTLP/gRPC server");
            server.shutdownNow();
        }
    }

    @Override
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        Server current;
        synchronized (this) {
            requireStarted();
            current = server;
        }
        return current.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        Server current;
        synchronized (this) {
            requireStarted();
            current = server;
        }
        current.awaitTermination();
    }

    private void requireStarted() {
        if (server == null) {
            throw new IllegalStateException("server not started");
        }
    }
}
