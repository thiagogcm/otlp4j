package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Graceful shutdown stops the Armeria receiver and releases its port, on both protocols. A
/// served request completes before shutdown, a request held in-flight when `shutdown(Duration)` is
/// called drains to its real result within the deadline, `shutdown(Duration)` settles within its
/// deadline and then refuses a fresh connection.
///
/// The server is built with a non-zero graceful-shutdown quiet period, so Armeria installs its real
/// drain tracker and waits for in-flight requests to complete before it tears connections down. A zero
/// quiet period would disable that tracker and abort in-flight requests at once, dropping telemetry on
/// every rolling restart, which the in-flight drain test below guards against.
@Timeout(60)
@DisplayName("Armeria graceful shutdown")
class ArmeriaGracefulShutdownTest {

    private final List<OtlpReceiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdown(Duration.ofSeconds(5)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    @DisplayName("gRPC: graceful shutdown completes and then refuses new connections")
    @Test
    void grpcGracefulStopsAccepting() throws Exception {
        gracefulStopsAccepting(true);
    }

    @DisplayName("HTTP: graceful shutdown completes and then refuses new connections")
    @Test
    void httpGracefulStopsAccepting() throws Exception {
        gracefulStopsAccepting(false);
    }

    @DisplayName("gRPC: a request held in-flight when shutdown begins drains to its real result")
    @Test
    void grpcGracefulDrainsInFlight() throws Exception {
        gracefulDrainsInFlight(true);
    }

    @DisplayName("HTTP: a request held in-flight when shutdown begins drains to its real result")
    @Test
    void httpGracefulDrainsInFlight() throws Exception {
        gracefulDrainsInFlight(false);
    }

    private void gracefulStopsAccepting(boolean grpc) throws Exception {
        var receiver = startReceiver(grpc, Consumers.accepting());
        var port = receiver.port();
        var batch = Fixtures.traceData(Fixtures.span("graceful", Span.Kind.SERVER));

        // A served request completes normally before shutdown begins.
        var exporter = exporterTo(grpc, port);
        assertThat(exporter.export(TRACES, batch).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);

        receiver.shutdown(Duration.ofSeconds(20)).toCompletableFuture().get(20, TimeUnit.SECONDS);

        assertRefused(grpc, port, batch);
    }

    private void gracefulDrainsInFlight(boolean grpc) throws Exception {
        var entered = new CountDownLatch(1);
        var gate = new CompletableFuture<DeliveryResult>();
        var receiver = startReceiver(grpc, Consumers.gated(entered::countDown, gate));
        var port = receiver.port();
        var batch = Fixtures.traceData(Fixtures.span("drain", Span.Kind.SERVER));
        var exporter = exporterTo(grpc, port);

        // Send a request that blocks in the consumer, so it is genuinely in-flight when shutdown begins.
        var inflight = exporter.export(TRACES, batch).toCompletableFuture();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        // Begin the graceful shutdown while the request is still held, then release it.
        var shutdown = receiver.shutdown(Duration.ofSeconds(20)).toCompletableFuture();
        gate.complete(DeliveryResult.accepted(Consumers.PROV));

        // The held request drains to its real Accepted result within the deadline, and shutdown settles.
        assertThat(inflight.get(20, TimeUnit.SECONDS)).isInstanceOf(DeliveryResult.Accepted.class);
        shutdown.get(20, TimeUnit.SECONDS);
    }

    private void assertRefused(boolean grpc, int port, TracesData batch) {
        var exporter = exporterTo(grpc, port);
        assertThatThrownBy(() -> exporter.export(TRACES, batch).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
    }

    private OtlpReceiver startReceiver(boolean grpc, SignalConsumer<TracesData> consumer) {
        var builder = grpc ? OtlpReceiver.grpcBuilder() : OtlpReceiver.httpBuilder();
        var receiver = builder.route(TRACES, consumer).ephemeralPort().build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpExporter exporterTo(boolean grpc, int port) {
        var exporter = (grpc ? OtlpExporter.grpcBuilder() : OtlpExporter.httpBuilder())
                .endpoint("localhost", port)
                .timeout(Duration.ofSeconds(5))
                .build().start();
        closeables.add(exporter);
        return exporter;
    }
}
