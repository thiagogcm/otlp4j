package dev.nthings.otlp4j.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpServer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Pins F11.4: the server stores the [dev.nthings.otlp4j.transport.common.AdmissionControl] it builds
/// in `start()` and closes it on shutdown, so its in-flight gauge registrations are released (no leak).
@Timeout(30)
@DisplayName("gRPC server closes admission control on shutdown")
class GrpcServerAdmissionCloseTest {

    private static final Dispatchers NO_OP = new Dispatchers() {
        @Override
        public CompletionStage<DeliveryResult> dispatch(TelemetryPayload payload) {
            return CompletableFuture.completedStage(DeliveryResult.accepted(Consumers.PROV));
        }
    };

    /// A registry that counts gauge registrations and how many were closed.
    private static final class CountingRegistry implements GatewayMetrics {
        final AtomicInteger registered = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();

        @Override
        public LongCounter counter(String name, String description) {
            return delta -> {};
        }

        @Override
        public Registration gauge(String name, String description, LongSupplier value) {
            registered.incrementAndGet();
            return closed::incrementAndGet;
        }
    }

    @DisplayName("a graceful shutdown closes the admission control (its gauges are unregistered)")
    @Test
    void shutdownClosesAdmission() throws Exception {
        var registry = new CountingRegistry();
        var server = new GrpcOtlpServer(ServerConfig.builder().setPort(0).build(), NO_OP, registry, 0);
        server.start();
        // A real registry always builds a counting control that registers the two in-flight gauges.
        assertThat(registry.registered.get()).isEqualTo(2);
        assertThat(registry.closed.get()).isZero();

        server.shutdown(java.time.Duration.ofSeconds(5)).toCompletableFuture().join();
        assertThat(registry.closed.get()).isEqualTo(2);
    }

    @DisplayName("a forceful shutdownNow also closes the admission control")
    @Test
    void shutdownNowClosesAdmission() throws Exception {
        var registry = new CountingRegistry();
        var server = new GrpcOtlpServer(ServerConfig.builder().setPort(0).build(), NO_OP, registry, 0);
        server.start();
        server.shutdownNow().toCompletableFuture().join();
        assertThat(registry.closed.get()).isEqualTo(2);
    }
}
