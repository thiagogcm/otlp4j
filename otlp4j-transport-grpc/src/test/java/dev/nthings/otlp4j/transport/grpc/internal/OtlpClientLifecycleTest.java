package dev.nthings.otlp4j.transport.grpc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Lifecycle contract for the OTLP/gRPC client's [GrpcOtlpClient#shutdown(Duration)]: the channel
/// is lazy, so no collector is needed to build a client and tear it down.
@Timeout(30)
@DisplayName("OtlpClient lifecycle (gRPC)")
class OtlpClientLifecycleTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, TRACES);

    private final List<ServerReceiver> receivers = new ArrayList<>();
    private final List<CompletableFuture<DeliveryResult>> gates = new ArrayList<>();

    @AfterEach
    void teardown() {
        // Release any held server-side handlers first so a blocked in-flight export can unwind.
        for (var gate : gates) {
            gate.complete(DeliveryResult.accepted(PROV));
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) {
                /* keep tearing down */ }
        }
    }

    private static ClientConfig config() {
        return ClientConfig.builder().setEndpoint("localhost", 4317).build();
    }

    @DisplayName("shutdown(timeout) drains the channel and completes cleanly")
    @Test
    void shutdownDrainsAndCompletes() {
        var client = new GrpcOtlpClient(config());
        assertThatCode(() -> client.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("the inherited close() routes through shutdown without throwing")
    @Test
    void closeRoutesThroughShutdown() {
        assertThatCode(() -> new GrpcOtlpClient(config()).close()).doesNotThrowAnyException();
    }

    @DisplayName("shutdown bounds a stuck in-flight export by the timeout instead of hanging on it")
    @Test
    void shutdownBoundsAnInFlightExportByTheTimeout() throws Exception {
        // A receiver whose handler never completes until teardown: it holds the export's RPC open on
        // the channel so the teardown wait actually has something to block on. Without an in-flight
        // export both teardown phases return instantly and the timeout budget is never exercised at
        // all (an idle client tears down whether or not the drain is bounded), so the in-flight
        // export is what turns this into a real regression guard for the bounded, remaining-budget
        // teardown: a drain that ignored the budget would hang here for the full 20s client timeout.
        var entered = new CountDownLatch(1);
        var gate = new CompletableFuture<DeliveryResult>();
        gates.add(gate);
        SignalConsumer<TelemetryPayload> gated = new SignalConsumer<>() {
            @Override
            public ConsumerCapabilities capabilities() {
                return ConsumerCapabilities.nonMutating();
            }

            @Override
            public java.util.concurrent.CompletionStage<DeliveryResult> consume(
                    ConsumeContext context, TelemetryPayload data) {
                entered.countDown();
                return gate;
            }
        };
        var receiver = OtlpGrpcReceiver.builder().ephemeralPort()
                .route(TRACES, gated)
                .build()
                .start();
        receivers.add(receiver);

        // Generous client timeout so the in-flight export would otherwise stay blocked far longer
        // than the small shutdown budget below.
        var client = new GrpcOtlpClient(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(20))
                .build());
        // Fire an export and leave it in flight; do not join it.
        client.export(TRACES, new TracesData(List.of()), PROV);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var timeout = Duration.ofMillis(500);
        var start = System.nanoTime();
        client.shutdown(timeout).toCompletableFuture().join();
        var elapsed = Duration.ofNanos(System.nanoTime() - start);

        // The two teardown phases split ONE budget (each gets the REMAINING budget, clamped at zero),
        // so an in-flight export is forced down within the timeout rather than awaited to completion.
        // A bounded teardown returns in roughly the budget; an unbounded one would run to the 20s
        // client timeout, so this five-second ceiling cleanly separates the two.
        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
    }
}
