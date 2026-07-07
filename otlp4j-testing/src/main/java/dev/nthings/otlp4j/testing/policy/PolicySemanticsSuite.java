package dev.nthings.otlp4j.testing.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.policy.ExportObservation.Outcome;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// The reusable exporter policy semantics suite, driven entirely through the [PolicyExporterUnderTest]
/// seam and the neutral vocabulary in this package. Each case pins one aspect of how the neutral
/// RetryPolicy, BackoffPolicy, and TimeoutPolicy behave when a real exporter drives a programmed server,
/// and runs on the consolidated Armeria transport. A concrete runner in otlp4j-conformance subclasses
/// this and supplies the seam; this class names no transport, client, or config type.
///
/// The exact backoff and folding arithmetic is pinned deterministically elsewhere (the
/// `ArmeriaRetryPolicyTest` unit test in the transport module asserts the retry rule and backoff
/// translation with tiny base waits). This suite proves the same behaviour is actually wired end to end
/// over a real transport and a real server: it asserts relative magnitude and ordering, not exact
/// milliseconds. Every case fixes an explicit, tiny backoff so no case silently runs the one-second
/// default. The load-bearing assertions are the deterministic attempt counts, orderings, and outcomes,
/// and the "at least the folded hint" lower bounds; the wall-clock upper bounds carry a fixed tolerance
/// to absorb virtual-thread scheduling, GC pauses, and loopback latency.
public abstract class PolicySemanticsSuite {

    /// Tolerance on upper-bound wall-clock assertions, absorbing scheduling and loopback latency.
    private static final Duration UPPER_TOLERANCE = Duration.ofMillis(500);

    /// Tolerance subtracted from a lower-bound wall-clock assertion, absorbing clock granularity while
    /// keeping the "at least the requested backoff" check meaningful.
    private static final Duration LOWER_TOLERANCE = Duration.ofMillis(30);

    /// A near-instant backoff for attempt-count and per-attempt cases that do not assert a gap.
    private static final BackoffPolicy INSTANT_BACKOFF =
            new BackoffPolicy(Duration.ofMillis(1), 1.0, 0.0, Duration.ofMillis(1));

    /// A generous timeout policy for cases that are not exercising a deadline, so a real loopback send
    /// never brushes against it.
    private static final TimeoutPolicy GENEROUS_TIMEOUT =
            new TimeoutPolicy(Duration.ofSeconds(2), Duration.ofSeconds(20));

    /// Supplies a fresh seam for one case. The concrete runner implements this by delegating to the one
    /// seam implementation; this suite never names it.
    ///
    /// @return the seam under test
    protected abstract PolicyExporterUnderTest newSeam();

    // --- RetryPolicy attempt counts ------------------------------------------------------------

    @DisplayName("a first-try success makes exactly one attempt")
    @Test
    void succeedsFirstTryNoRetry() {
        var obs = run(retry(5, INSTANT_BACKOFF, true), GENEROUS_TIMEOUT, List.of(new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(1);
        assertThat(obs.outcome()).isEqualTo(Outcome.ACCEPTED);
        // A first-try success emits no retry: the real client's onRetry listener never fires. Reading
        // zero here pins that the wiring does not over-count the first attempt as a retry.
        assertThat(obs.retries()).isZero();
    }

    @DisplayName("retries then succeeds within the attempt budget")
    @Test
    void exhaustsSomeAttemptsThenSucceeds() {
        var obs = run(retry(5, INSTANT_BACKOFF, true), GENEROUS_TIMEOUT, List.of(
                new ProgrammedResponse.Unavailable(),
                new ProgrammedResponse.Unavailable(),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(3);
        assertThat(obs.outcome()).isEqualTo(Outcome.ACCEPTED);
        // Three attempts means exactly two retries beyond the first, emitted through the real client's
        // retry listener into the exporter's retry counter.
        assertThat(obs.retries()).isEqualTo(2);
    }

    @DisplayName("maxAttempts counts total tries, not retries")
    @Test
    void exhaustsAllAttemptsAndFails() {
        var obs = run(retry(5, INSTANT_BACKOFF, true), GENEROUS_TIMEOUT, repeat(new ProgrammedResponse.Unavailable(), 5));
        assertThat(obs.attempts()).isEqualTo(5);
        assertThat(obs.outcome()).isEqualTo(Outcome.RETRYABLE_REJECTED);
        // Exhausting all five attempts emits exactly four retries. Asserting maxAttempts - 1 against the
        // exporter's retry counter pins the real client's onRetry to recordRetry wiring end to end: a
        // dropped, doubled, or off-by-one listener would move this number.
        assertThat(obs.retries()).isEqualTo(4);
    }

    @DisplayName("maxAttempts of one disables retry")
    @Test
    void maxAttemptsOneDisablesRetry() {
        var obs = run(retry(1, INSTANT_BACKOFF, true), GENEROUS_TIMEOUT, List.of(
                new ProgrammedResponse.Unavailable(),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(1);
        assertThat(obs.outcome()).isEqualTo(Outcome.RETRYABLE_REJECTED);
    }

    @DisplayName("a permanent error is not retried")
    @Test
    void permanentIsNotRetried() {
        var obs = run(retry(5, INSTANT_BACKOFF, true), GENEROUS_TIMEOUT, List.of(
                new ProgrammedResponse.Permanent(),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(1);
        assertThat(obs.outcome()).isEqualTo(Outcome.PERMANENT_REJECTED);
    }

    @DisplayName("gRPC negative pushback stops retry")
    @Test
    void grpcNegativePushbackStopsRetry() {
        try (var seam = newSeam()) {
            assumeTrue(seam.protocol() == Protocol.GRPC, "negative pushback is a gRPC behaviour");
            var obs = seam.runOnce(new ExportProgram(
                    retry(5, INSTANT_BACKOFF, true),
                    GENEROUS_TIMEOUT,
                    List.of(new ProgrammedResponse.ThrottleNoRetry(), new ProgrammedResponse.Ok())));
            assertThat(obs.attempts()).isEqualTo(1);
            assertThat(obs.outcome()).isEqualTo(Outcome.PERMANENT_REJECTED);
        }
    }

    // --- BackoffPolicy bounds ------------------------------------------------------------------

    @DisplayName("the first retry gap respects the initial delay")
    @Test
    void firstRetryGapRespectsInitialDelay() {
        var backoff = new BackoffPolicy(Duration.ofMillis(50), 2.0, 0.0, Duration.ofMillis(400));
        var obs = run(retry(5, backoff, true), GENEROUS_TIMEOUT, List.of(
                new ProgrammedResponse.Unavailable(),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(2);
        assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(50).minus(LOWER_TOLERANCE));
        assertThat(firstGap(obs)).isLessThanOrEqualTo(Duration.ofMillis(400).plus(UPPER_TOLERANCE));
    }

    @DisplayName("gaps grow toward the cap and never exceed it")
    @Test
    void gapsGrowTowardCapAndNeverExceedIt() {
        // Multiplier 5 makes the fourth uncapped gap (20 * 5^3 = 2500ms) far exceed the cap-plus-tolerance
        // bound (120 + 500 = 620ms), so a backoff that dropped the max-interval cap would fail this
        // assertion. A gentler multiplier leaves every uncapped gap under the tolerance, which is why the
        // earlier, looser choice could not discriminate a missing cap.
        var backoff = new BackoffPolicy(Duration.ofMillis(20), 5.0, 0.0, Duration.ofMillis(120));
        var obs = run(retry(6, backoff, true), GENEROUS_TIMEOUT, append(
                repeat(new ProgrammedResponse.Unavailable(), 4), new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(5);
        assertThat(obs.interAttemptGaps()).hasSize(4);
        assertEveryGapAtMost(obs, Duration.ofMillis(120));
    }

    @DisplayName("the backoff cap is honored under a large multiplier")
    @Test
    void backoffCapHonoredUnderLargeMultiplier() {
        var backoff = new BackoffPolicy(Duration.ofMillis(20), 10.0, 0.0, Duration.ofMillis(80));
        var obs = run(retry(5, backoff, true), GENEROUS_TIMEOUT, append(
                repeat(new ProgrammedResponse.Unavailable(), 3), new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(4);
        assertEveryGapAtMost(obs, Duration.ofMillis(80));
    }

    // --- Retry-after folding -------------------------------------------------------------------

    @DisplayName("a server throttle hint is honored when the policy respects it")
    @Test
    void throttleHintHonoredWhenRespected() {
        var backoff = new BackoffPolicy(Duration.ofMillis(20), 2.0, 0.0, Duration.ofMillis(80));
        var obs = run(retry(5, backoff, true), GENEROUS_TIMEOUT, List.of(
                new ProgrammedResponse.Throttle(Duration.ofMillis(500)),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(2);
        assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(500).minus(LOWER_TOLERANCE));
    }

    @DisplayName("a server throttle hint is ignored when the policy disables it")
    @Test
    void throttleHintIgnoredWhenDisabled() {
        var backoff = new BackoffPolicy(Duration.ofMillis(20), 2.0, 0.0, Duration.ofMillis(80));
        var obs = run(retry(5, backoff, false), GENEROUS_TIMEOUT, List.of(
                new ProgrammedResponse.Throttle(Duration.ofMillis(500)),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(2);
        assertThat(firstGap(obs)).isLessThanOrEqualTo(Duration.ofMillis(80).plus(UPPER_TOLERANCE));
    }

    @DisplayName("HTTP honors an RFC 1123 date Retry-After")
    @Test
    void httpHonorsDateRetryAfter() {
        try (var seam = newSeam()) {
            assumeTrue(seam.protocol() == Protocol.HTTP, "an absolute-date Retry-After is an HTTP behaviour");
            var backoff = new BackoffPolicy(Duration.ofMillis(20), 2.0, 0.0, Duration.ofMillis(80));
            var obs = seam.runOnce(new ExportProgram(retry(5, backoff, true), GENEROUS_TIMEOUT, List.of(
                    new ProgrammedResponse.ThrottleDate(Duration.ofMillis(500)),
                    new ProgrammedResponse.Ok())));
            assertThat(obs.attempts()).isEqualTo(2);
            assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(500).minus(LOWER_TOLERANCE));
        }
    }

    // --- RetryInfo handling status (capability-gated) ------------------------------------------

    @DisplayName("RetryInfo is folded only where the transport honors it")
    @Test
    void retryInfoFoldedWhereHonored() {
        try (var seam = newSeam()) {
            var backoff = new BackoffPolicy(Duration.ofMillis(20), 2.0, 0.0, Duration.ofMillis(80));
            var obs = seam.runOnce(new ExportProgram(retry(5, backoff, true), GENEROUS_TIMEOUT, List.of(
                    new ProgrammedResponse.RetryInfo(Duration.ofMillis(500)),
                    new ProgrammedResponse.Ok())));
            assertThat(obs.attempts()).isEqualTo(2);
            if (seam.honorsGrpcRetryInfo()) {
                assertThat(firstGap(obs)).isGreaterThanOrEqualTo(Duration.ofMillis(500).minus(LOWER_TOLERANCE));
            } else {
                // The legacy gRPC client ignores grpc-status-details-bin RetryInfo and falls back to its
                // own backoff, so the gap stays at the computed cap. This pins the documented Phase 4 gap.
                assertThat(firstGap(obs)).isLessThanOrEqualTo(Duration.ofMillis(80).plus(UPPER_TOLERANCE));
            }
        }
    }

    // --- TimeoutPolicy per-attempt and total ---------------------------------------------------

    @DisplayName("the per-attempt deadline abandons a single hung attempt, then retries")
    @Test
    void perAttemptDeadlineAbandonsOneAttempt() {
        var timeout = new TimeoutPolicy(Duration.ofMillis(200), Duration.ofSeconds(5));
        var obs = run(retry(5, INSTANT_BACKOFF, true), timeout, List.of(
                new ProgrammedResponse.Hang(Duration.ofMillis(500)),
                new ProgrammedResponse.Ok()));
        assertThat(obs.attempts()).isEqualTo(2);
        assertThat(obs.outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(obs.totalElapsed()).isLessThan(Duration.ofSeconds(3));
    }

    @DisplayName("the total deadline abandons the whole retrying delivery")
    @Test
    void totalDeadlineAbandonsWholeDelivery() {
        try (var seam = newSeam()) {
            var timeout = new TimeoutPolicy(Duration.ofMillis(150), Duration.ofMillis(600));
            var obs = seam.runOnce(new ExportProgram(
                    retry(100, INSTANT_BACKOFF, true),
                    timeout,
                    repeat(new ProgrammedResponse.Hang(Duration.ofMillis(300)), 20)));
            assertThat(obs.outcome()).isEqualTo(Outcome.TIMED_OUT);
            // Near total (600ms), not near perAttempt (150ms) and not near 100 attempts of 150ms: this
            // is what proves total is wired and distinct from perAttempt.
            assertThat(obs.totalElapsed()).isGreaterThanOrEqualTo(Duration.ofMillis(500));
            assertThat(obs.totalElapsed()).isLessThanOrEqualTo(Duration.ofMillis(600).plus(UPPER_TOLERANCE));
            if (seam.cancelsRetriesAtTotalDeadline()) {
                // Only a transport that cancels in-flight retry scheduling can promise no attempt arrives
                // after the total deadline; legacy cannot, and the fake server's over-script tolerance
                // absorbs any straggler.
                assertThat(obs.attempts())
                        .isLessThanOrEqualTo(4)
                        .isGreaterThanOrEqualTo(1);
            }
        }
    }

    @DisplayName("total equal to perAttempt collapses to a single-budget timeout")
    @Test
    void totalEqualToPerAttemptCollapses() {
        try (var seam = newSeam()) {
            // perAttempt equal to total, so the two bounds collapse to a single budget. The budget is a
            // full second rather than a couple hundred milliseconds so that first-connection setup on a
            // cold transport (a fresh gRPC channel must complete TCP connect and the HTTP/2 preface before
            // any attempt reaches the server) cannot itself exhaust the deadline before the attempt floor
            // is met. A tighter budget made this case race connection setup and record zero arrivals.
            var budget = Duration.ofSeconds(1);
            var timeout = new TimeoutPolicy(budget, budget);
            // RetryPolicy.defaults() so the case exercises the default-shaped config where the two bounds
            // coincide, matching OtlpExporterConfig.defaults.
            var obs = seam.runOnce(new ExportProgram(
                    RetryPolicy.defaults(),
                    timeout,
                    List.of(new ProgrammedResponse.Hang(Duration.ofSeconds(3)))));
            assertThat(obs.outcome()).isEqualTo(Outcome.TIMED_OUT);
            assertThat(obs.totalElapsed()).isLessThanOrEqualTo(budget.plus(UPPER_TOLERANCE));
            // On legacy the exact straggler count is inherent no-cancel behaviour, which is precisely
            // what cancelsRetriesAtTotalDeadline() gates; pinning it to one on legacy would assert the
            // absence of a documented limitation, so assert only the floor there.
            assertThat(obs.attempts()).isGreaterThanOrEqualTo(1);
            if (seam.cancelsRetriesAtTotalDeadline()) {
                assertThat(obs.attempts()).isEqualTo(1);
            }
        }
    }

    // --- helpers -------------------------------------------------------------------------------

    /// Runs one program on a fresh seam and returns the observation.
    private ExportObservation run(RetryPolicy retry, TimeoutPolicy timeout, List<ProgrammedResponse> script) {
        try (var seam = newSeam()) {
            return seam.runOnce(new ExportProgram(retry, timeout, script));
        }
    }

    private static RetryPolicy retry(int maxAttempts, BackoffPolicy backoff, boolean respectServerRetryAfter) {
        return new RetryPolicy(maxAttempts, backoff, respectServerRetryAfter);
    }

    private static Duration firstGap(ExportObservation obs) {
        assertThat(obs.interAttemptGaps()).isNotEmpty();
        return obs.interAttemptGaps().get(0);
    }

    private static void assertEveryGapAtMost(ExportObservation obs, Duration cap) {
        for (var gap : obs.interAttemptGaps()) {
            assertThat(gap).isLessThanOrEqualTo(cap.plus(UPPER_TOLERANCE));
        }
    }

    private static List<ProgrammedResponse> repeat(ProgrammedResponse response, int count) {
        var list = new ArrayList<ProgrammedResponse>(count);
        for (int i = 0; i < count; i++) {
            list.add(response);
        }
        return List.copyOf(list);
    }

    private static List<ProgrammedResponse> append(List<ProgrammedResponse> head, ProgrammedResponse tail) {
        var list = new ArrayList<>(head);
        list.add(tail);
        return List.copyOf(list);
    }
}
