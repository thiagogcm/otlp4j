package dev.nthings.otlp4j.transport.otlp;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.telemetry.LoggingGatewayMetrics;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.transport.otlp.ScriptedCollector.Step;
import dev.nthings.otlp4j.transport.otlp.ScriptedCollector.Wire;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// End-to-end retry, backoff, and deadline translation over the real Armeria client, driven against an
/// in-module scripted collector. These exercise the same wiring the Phase 3 policy semantics suite
/// drives in Lot 3, so they de-risk that lot: exact attempt counts, exact retry-instrument counts, the
/// gRPC negative-pushback stop-retry, positive pushback and `RetryInfo` folding, HTTP `Retry-After`
/// folding, and the per-attempt and total deadlines.
@Timeout(120)
@DisplayName("Armeria retry policy end to end")
class ArmeriaRetryPolicyTest {

    private static final String RETRIES_COUNTER = "otlp4j.exporter.export.retries";

    /// A near-instant backoff for attempt-count cases that do not assert a gap.
    private static final BackoffPolicy INSTANT = new BackoffPolicy(
            Duration.ofMillis(1), 1.0, 0.0, Duration.ofMillis(1));

    /// A small backoff whose cap is well below the server hints, so a folded hint dominates the gap.
    private static final BackoffPolicy SMALL = new BackoffPolicy(
            Duration.ofMillis(20), 2.0, 0.0, Duration.ofMillis(50));

    private static final Duration PER_ATTEMPT = Duration.ofSeconds(3);
    private static final Duration TOTAL = Duration.ofSeconds(20);

    // --- attempt counts and the retry instrument -----------------------------------------------

    @DisplayName("gRPC: a first-try success makes one attempt and emits no retry")
    @Test
    void grpcFirstTrySucceeds() {
        var obs = run(Wire.GRPC, retry(5, INSTANT, true), PER_ATTEMPT, TOTAL, List.of(new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(1);
        assertThat(obs.retries).isZero();
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("gRPC: retries then succeeds, counting exactly the retries beyond the first")
    @Test
    void grpcRetriesThenSucceeds() {
        var obs = run(Wire.GRPC, retry(5, INSTANT, true), PER_ATTEMPT, TOTAL, List.of(
                new Step.Unavailable(), new Step.Unavailable(), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(3);
        assertThat(obs.retries).isEqualTo(2);
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("gRPC: exhausting all attempts emits maxAttempts - 1 retries and fails")
    @Test
    void grpcExhaustsAllAttempts() {
        var obs = run(Wire.GRPC, retry(5, INSTANT, true), PER_ATTEMPT, TOTAL,
                repeat(new Step.Unavailable(), 6));
        assertThat(obs.attempts).isEqualTo(5);
        assertThat(obs.retries).isEqualTo(4);
        assertThat(obs.failure).isNotNull();
        assertThat(unwrap(obs.failure)).isNotInstanceOf(TimeoutException.class);
    }

    @DisplayName("HTTP: retries then succeeds, counting exactly the retries beyond the first")
    @Test
    void httpRetriesThenSucceeds() {
        var obs = run(Wire.HTTP, retry(5, INSTANT, true), PER_ATTEMPT, TOTAL, List.of(
                new Step.Unavailable(), new Step.Unavailable(), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(3);
        assertThat(obs.retries).isEqualTo(2);
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("HTTP: exhausting all attempts emits maxAttempts - 1 retries and fails")
    @Test
    void httpExhaustsAllAttempts() {
        var obs = run(Wire.HTTP, retry(5, INSTANT, true), PER_ATTEMPT, TOTAL,
                repeat(new Step.Unavailable(), 6));
        assertThat(obs.attempts).isEqualTo(5);
        assertThat(obs.retries).isEqualTo(4);
        assertThat(obs.failure).isNotNull();
    }

    // --- gRPC pushback and RetryInfo -----------------------------------------------------------

    @DisplayName("gRPC: a negative pushback stops retry at a single attempt")
    @Test
    void grpcNegativePushbackStops() {
        var obs = run(Wire.GRPC, retry(5, INSTANT, true), PER_ATTEMPT, TOTAL, List.of(
                new Step.StopRetry(), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(1);
        assertThat(obs.retries).isZero();
        assertThat(obs.failure).isNotNull();
        assertThat(unwrap(obs.failure)).isNotInstanceOf(TimeoutException.class);
    }

    @DisplayName("gRPC: a positive pushback drives the next backoff")
    @Test
    void grpcPositivePushbackFolded() {
        var obs = run(Wire.GRPC, retry(5, SMALL, true), PER_ATTEMPT, TOTAL, List.of(
                new Step.Throttle(Duration.ofMillis(400)), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(2);
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(360));
    }

    @DisplayName("gRPC: a RetryInfo detail drives the next backoff (interop gap closed)")
    @Test
    void grpcRetryInfoFolded() {
        var obs = run(Wire.GRPC, retry(5, SMALL, true), PER_ATTEMPT, TOTAL, List.of(
                new Step.RetryInfo(Duration.ofMillis(400)), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(2);
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(360));
    }

    @DisplayName("gRPC: a positive pushback is ignored when the policy disables server hints")
    @Test
    void grpcPushbackIgnoredWhenDisabled() {
        var obs = run(Wire.GRPC, retry(5, SMALL, false), PER_ATTEMPT, TOTAL, List.of(
                new Step.Throttle(Duration.ofMillis(400)), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(2);
        // The 50ms backoff cap governs, not the 400ms hint.
        assertThat(firstGap(obs)).isLessThanOrEqualTo(Duration.ofMillis(400));
    }

    // --- HTTP Retry-After ----------------------------------------------------------------------

    @DisplayName("HTTP: a numeric Retry-After drives the next backoff when respected")
    @Test
    void httpRetryAfterFolded() {
        var obs = run(Wire.HTTP, retry(5, SMALL, true), PER_ATTEMPT, TOTAL, List.of(
                new Step.Throttle(Duration.ofSeconds(1)), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(2);
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
        // Retry-After granularity is whole seconds; at least ~1s dominates the 50ms backoff cap.
        assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(900));
    }

    @DisplayName("HTTP: a Retry-After is ignored when the policy disables server hints")
    @Test
    void httpRetryAfterIgnoredWhenDisabled() {
        var obs = run(Wire.HTTP, retry(5, SMALL, false), PER_ATTEMPT, TOTAL, List.of(
                new Step.Throttle(Duration.ofSeconds(1)), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(2);
        assertThat(firstGap(obs)).isLessThanOrEqualTo(Duration.ofMillis(600));
    }

    // --- deadlines -----------------------------------------------------------------------------

    @DisplayName("the per-attempt deadline abandons one hung attempt, then retries")
    @Test
    void perAttemptDeadlineRetries() {
        var obs = run(Wire.GRPC, retry(5, INSTANT, true),
                Duration.ofMillis(300), Duration.ofSeconds(10), List.of(
                        new Step.Hang(Duration.ofMillis(800)), new Step.Ok()));
        assertThat(obs.attempts).isEqualTo(2);
        assertThat(obs.result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(obs.elapsed).isLessThan(Duration.ofSeconds(5));
    }

    @DisplayName("the total deadline abandons the whole retrying delivery as a timeout")
    @Test
    void totalDeadlineTimesOut() {
        var obs = run(Wire.GRPC, retry(100, INSTANT, true),
                Duration.ofMillis(150), Duration.ofMillis(600),
                repeat(new Step.Hang(Duration.ofMillis(300)), 20));
        assertThat(unwrap(obs.failure)).isInstanceOf(TimeoutException.class);
        assertThat(obs.elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(400));
        assertThat(obs.elapsed).isLessThanOrEqualTo(Duration.ofSeconds(3));
    }

    // --- harness -------------------------------------------------------------------------------

    private record Observation(
            int attempts, int retries, Duration elapsed,
            @Nullable DeliveryResult result, @Nullable Throwable failure, List<Duration> gaps) {}

    private Observation run(
            Wire wire, RetryPolicy retry, Duration perAttempt, Duration total, List<Step> script) {
        try (var collector = ScriptedCollector.start(wire, script)) {
            var telemetry = new LoggingGatewayMetrics();
            var config = ClientConfig.builder()
                    .setHost("127.0.0.1")
                    .setPort(collector.port())
                    .setTimeout(perAttempt)
                    .setTotalTimeout(total)
                    .setRetryPolicy(retry)
                    .build();
            var exporter = switch (wire) {
                case GRPC -> OtlpArmeriaExporter.grpcBuilder().setConfig(config).telemetry(telemetry).build();
                case HTTP -> OtlpArmeriaExporter.httpBuilder().setConfig(config).telemetry(telemetry).build();
            };
            DeliveryResult result = null;
            Throwable failure = null;
            long start = System.nanoTime();
            try {
                result = exporter.export(SignalDescriptor.TRACES, batch()).toCompletableFuture().join();
            } catch (CompletionException | CancellationException e) {
                failure = e;
            }
            var elapsed = Duration.ofNanos(System.nanoTime() - start);
            try {
                exporter.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
            } catch (RuntimeException ignored) {
                // teardown already interrupts the transport; the snapshot below stays valid
            }
            return new Observation(
                    collector.attempts(),
                    (int) telemetry.counterValue(RETRIES_COUNTER),
                    elapsed,
                    result,
                    failure,
                    collector.interAttemptGaps());
        }
    }

    private static RetryPolicy retry(int maxAttempts, BackoffPolicy backoff, boolean respect) {
        return new RetryPolicy(maxAttempts, backoff, respect);
    }

    private static TracesData batch() {
        return TransportFixtures.richTraceData();
    }

    private static Duration firstGap(Observation obs) {
        assertThat(obs.gaps).isNotEmpty();
        return obs.gaps.get(0);
    }

    private static @Nullable Throwable unwrap(@Nullable Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static List<Step> repeat(Step step, int count) {
        var list = new java.util.ArrayList<Step>(count);
        for (int i = 0; i < count; i++) {
            list.add(step);
        }
        return List.copyOf(list);
    }
}
