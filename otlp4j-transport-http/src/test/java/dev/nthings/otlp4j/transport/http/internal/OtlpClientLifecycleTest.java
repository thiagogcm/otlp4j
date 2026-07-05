package dev.nthings.otlp4j.transport.http.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.SignalBatch;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.transport.http.OtlpHttpReceiver;
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

/// Lifecycle contract for the OTLP/HTTP client's [HttpOtlpClient#shutdown(Duration)]: no collector
/// is needed to build a client and tear it down.
@Timeout(30)
@DisplayName("OtlpClient lifecycle (HTTP)")
class OtlpClientLifecycleTest {

    private final List<Receiver> receivers = new ArrayList<>();
    private final List<CompletableFuture<ConsumeResult>> gates = new ArrayList<>();

    @AfterEach
    void teardown() {
        // Release any held server-side handlers first so a blocked in-flight export can unwind.
        for (var gate : gates) {
            gate.complete(ConsumeResult.accepted());
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) {
                /* keep tearing down */ }
        }
    }

    private static ClientConfig config() {
        return ClientConfig.builder().setEndpoint("localhost", 4318).build();
    }

    @DisplayName("shutdown(timeout) drains the per-export executor and completes cleanly")
    @Test
    void shutdownDrainsAndCompletes() {
        var client = new HttpOtlpClient(config());
        assertThatCode(() -> client.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("the inherited close() routes through shutdown without throwing")
    @Test
    void closeRoutesThroughShutdown() {
        assertThatCode(() -> new HttpOtlpClient(config()).close()).doesNotThrowAnyException();
    }

    @DisplayName("shutdown bounds a stuck in-flight export by the timeout instead of hanging on it")
    @Test
    void shutdownBoundsAnInFlightExportByTheTimeout() throws Exception {
        // A receiver whose handler never completes until teardown: it holds the export's request open
        // so the per-export executor drain actually has something to block on. Without an in-flight
        // export the drain returns instantly and the timeout budget is never exercised at all (an
        // idle client tears down whether or not the drain is bounded), so the in-flight export is
        // what turns this into a real regression guard for the bounded, remaining-budget drain: a
        // drain that ignored the budget would hang here for the full 20s client timeout.
        var entered = new CountDownLatch(1);
        var gate = new CompletableFuture<ConsumeResult>();
        gates.add(gate);
        var receiver = OtlpHttpReceiver.builder().ephemeralPort()
                .onTraces(traces -> {
                    entered.countDown();
                    return gate;
                })
                .build()
                .start();
        receivers.add(receiver);

        // Generous client timeout so the in-flight export would otherwise stay blocked far longer
        // than the small shutdown budget below.
        var client = new HttpOtlpClient(ClientConfig.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(20))
                .build());
        // Fire an export and leave it in flight; do not join it.
        client.export(new SignalBatch<>(Signal.TRACES, new TracesData(List.of())));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var timeout = Duration.ofMillis(500);
        var start = System.nanoTime();
        client.shutdown(timeout).toCompletableFuture().join();
        var elapsed = Duration.ofNanos(System.nanoTime() - start);

        // The executor drain gets the remaining budget clamped at zero, so an in-flight export is
        // interrupted within the timeout rather than awaited to completion. A bounded drain returns
        // in roughly the budget; an unbounded one would run to the 20s client timeout, so this
        // five-second ceiling cleanly separates the two.
        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
    }
}
