package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.retry.Backoff;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.transport.internal.ArmeriaRetryRules.Decision;
import dev.nthings.otlp4j.transport.internal.ArmeriaRetryRules.Kind;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Deterministic unit tests pinning the retry-translation decision logic: the gRPC and HTTP retry
/// precedence, the backoff translation, and the `grpc-status-details-bin` `RetryInfo` round trip. The
/// end-to-end wiring over a real Armeria client and server is exercised separately in
/// `ArmeriaRetryPolicyTest`; these tests pin the arithmetic and the precedence order in isolation.
@DisplayName("Armeria retry translation")
class ArmeriaRetryRulesTest {

    /// The retryable gRPC UNAVAILABLE code and a non-retryable INVALID_ARGUMENT code.
    private static final int UNAVAILABLE = 14;
    private static final int INVALID_ARGUMENT = 3;

    @Nested
    @DisplayName("gRPC precedence")
    class GrpcPrecedence {

        @DisplayName("RetryInfo wins over a positive pushback trailer when both are present")
        @Test
        void retryInfoWinsOverPushback() {
            var decision = ArmeriaRetryRules.decideGrpc(
                    UNAVAILABLE, 200L, Duration.ofMillis(750), false, true);
            assertThat(decision.kind()).isEqualTo(Kind.RETRY_DELAY);
            assertThat(decision.delay()).isEqualTo(Duration.ofMillis(750));
        }

        @DisplayName("a positive pushback is used when there is no RetryInfo")
        @Test
        void pushbackUsedWithoutRetryInfo() {
            var decision = ArmeriaRetryRules.decideGrpc(UNAVAILABLE, 300L, null, false, true);
            assertThat(decision.kind()).isEqualTo(Kind.RETRY_DELAY);
            assertThat(decision.delay()).isEqualTo(Duration.ofMillis(300));
        }

        @DisplayName("a negative pushback stops retry, overriding a positive RetryInfo")
        @Test
        void negativePushbackStopsEvenWithRetryInfo() {
            var decision = ArmeriaRetryRules.decideGrpc(
                    UNAVAILABLE, -1L, Duration.ofMillis(750), false, true);
            assertThat(decision.kind()).isEqualTo(Kind.STOP);
        }

        @DisplayName("a negative pushback stops retry even when the policy disables server hints")
        @Test
        void negativePushbackStopsWithHintsDisabled() {
            var decision = ArmeriaRetryRules.decideGrpc(UNAVAILABLE, -5L, null, false, false);
            assertThat(decision.kind()).isEqualTo(Kind.STOP);
        }

        @DisplayName("a retryable status with no hint uses the default backoff")
        @Test
        void retryableWithoutHintUsesDefaultBackoff() {
            var decision = ArmeriaRetryRules.decideGrpc(UNAVAILABLE, null, null, false, true);
            assertThat(decision.kind()).isEqualTo(Kind.RETRY_DEFAULT);
        }

        @DisplayName("positive hints are ignored when the policy disables them, but the retry still runs")
        @Test
        void positiveHintsIgnoredWhenDisabled() {
            var decision = ArmeriaRetryRules.decideGrpc(
                    UNAVAILABLE, 300L, Duration.ofMillis(750), false, false);
            assertThat(decision.kind()).isEqualTo(Kind.RETRY_DEFAULT);
        }

        @DisplayName("a non-retryable status is not retried")
        @Test
        void nonRetryableStatusStops() {
            var decision = ArmeriaRetryRules.decideGrpc(INVALID_ARGUMENT, null, null, false, true);
            assertThat(decision.kind()).isEqualTo(Kind.NO_RETRY);
        }

        @DisplayName("an OK status is not retried")
        @Test
        void okStatusStops() {
            var decision = ArmeriaRetryRules.decideGrpc(0, null, null, false, true);
            assertThat(decision.kind()).isEqualTo(Kind.NO_RETRY);
        }

        @DisplayName("a transport failure with no status retries on the default backoff")
        @Test
        void transportFailureRetriesDefault() {
            var decision = ArmeriaRetryRules.decideGrpc(null, null, null, true, true);
            assertThat(decision.kind()).isEqualTo(Kind.RETRY_DEFAULT);
        }
    }

    @Nested
    @DisplayName("HTTP precedence")
    class HttpPrecedence {

        @DisplayName("a retryable status retries on the default backoff (Retry-After is folded in the rule)")
        @Test
        void retryableStatusRetries() {
            assertThat(ArmeriaRetryRules.decideHttp(503, false).kind()).isEqualTo(Kind.RETRY_DEFAULT);
            assertThat(ArmeriaRetryRules.decideHttp(429, false).kind()).isEqualTo(Kind.RETRY_DEFAULT);
            assertThat(ArmeriaRetryRules.decideHttp(504, false).kind()).isEqualTo(Kind.RETRY_DEFAULT);
        }

        @DisplayName("a non-retryable status is not retried")
        @Test
        void nonRetryableStatusStops() {
            assertThat(ArmeriaRetryRules.decideHttp(500, false).kind()).isEqualTo(Kind.NO_RETRY);
            assertThat(ArmeriaRetryRules.decideHttp(400, false).kind()).isEqualTo(Kind.NO_RETRY);
        }

        @DisplayName("a 2xx status is not retried")
        @Test
        void successStops() {
            assertThat(ArmeriaRetryRules.decideHttp(200, false).kind()).isEqualTo(Kind.NO_RETRY);
        }

        @DisplayName("a transport failure retries on the default backoff")
        @Test
        void transportFailureRetriesDefault() {
            assertThat(ArmeriaRetryRules.decideHttp(null, true).kind()).isEqualTo(Kind.RETRY_DEFAULT);
        }
    }

    @Nested
    @DisplayName("Retry-After parsing")
    class RetryAfterParsing {

        @DisplayName("a positive numeric delta-seconds value parses to that many seconds")
        @Test
        void numericSeconds() {
            assertThat(ArmeriaRetryRules.parseRetryAfter("3")).isEqualTo(Duration.ofSeconds(3));
            assertThat(ArmeriaRetryRules.parseRetryAfter("  5 ")).isEqualTo(Duration.ofSeconds(5));
        }

        @DisplayName("a zero or negative numeric value yields no delay")
        @Test
        void numericNonPositive() {
            assertThat(ArmeriaRetryRules.parseRetryAfter("0")).isNull();
            assertThat(ArmeriaRetryRules.parseRetryAfter("-4")).isNull();
        }

        @DisplayName("an RFC 1123 absolute date in the future parses to a positive delay")
        @Test
        void absoluteDateInFuture() {
            var at = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).plusSeconds(30);
            var header = at.format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME);
            var delay = ArmeriaRetryRules.parseRetryAfter(header);
            assertThat(delay).isNotNull();
            // Whole-second header resolution, so the delay lands in a generous window around 30s.
            assertThat(delay).isBetween(Duration.ofSeconds(20), Duration.ofSeconds(31));
        }

        @DisplayName("an RFC 1123 absolute date in the past yields no delay")
        @Test
        void absoluteDateInPast() {
            var at = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).minusSeconds(30);
            var header = at.format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME);
            assertThat(ArmeriaRetryRules.parseRetryAfter(header)).isNull();
        }

        @DisplayName("an absent or unparseable value yields no delay")
        @Test
        void absentOrUnparseable() {
            assertThat(ArmeriaRetryRules.parseRetryAfter(null)).isNull();
            assertThat(ArmeriaRetryRules.parseRetryAfter("not-a-date")).isNull();
        }
    }

    @Nested
    @DisplayName("backoff translation")
    class BackoffTranslation {

        @DisplayName("an exponential backoff grows by the multiplier and never exceeds the cap")
        @Test
        void exponentialGrowsAndCaps() {
            var policy = new BackoffPolicy(Duration.ofMillis(20), 2.0, 0.0, Duration.ofMillis(80));
            Backoff backoff = ArmeriaRetryRules.backoff(policy);
            assertThat(backoff.nextDelayMillis(1)).isEqualTo(20);
            assertThat(backoff.nextDelayMillis(2)).isEqualTo(40);
            assertThat(backoff.nextDelayMillis(3)).isEqualTo(80);
            // Capped: the fourth uncapped delay (160) is clamped to the 80ms max.
            assertThat(backoff.nextDelayMillis(4)).isEqualTo(80);
        }

        @DisplayName("a multiplier of one is a constant backoff at the initial delay")
        @Test
        void multiplierOneIsConstant() {
            var policy = new BackoffPolicy(Duration.ofMillis(30), 1.0, 0.0, Duration.ofMillis(30));
            Backoff backoff = ArmeriaRetryRules.backoff(policy);
            assertThat(backoff.nextDelayMillis(1)).isEqualTo(30);
            assertThat(backoff.nextDelayMillis(2)).isEqualTo(30);
            assertThat(backoff.nextDelayMillis(5)).isEqualTo(30);
        }
    }

    @Nested
    @DisplayName("RetryInfo codec")
    class RetryInfoCodecRoundTrip {

        @DisplayName("an encoded RetryInfo round-trips through the raw bytes")
        @Test
        void roundTripsBytes() {
            var bytes = RetryInfoCodec.encodeRetryInfo(UNAVAILABLE, "throttled", Duration.ofMillis(1500));
            assertThat(RetryInfoCodec.decodeRetryDelay(bytes)).isEqualTo(Duration.ofMillis(1500));
        }

        @DisplayName("an encoded RetryInfo round-trips through the base64 trailer string")
        @Test
        void roundTripsBase64() {
            var bytes = RetryInfoCodec.encodeRetryInfo(UNAVAILABLE, "throttled", Duration.ofMillis(2500));
            var trailer = Base64.getEncoder().encodeToString(bytes);
            assertThat(RetryInfoCodec.decodeRetryDelay(trailer)).isEqualTo(Duration.ofMillis(2500));
        }

        @DisplayName("a sub-second RetryInfo delay preserves its nanos")
        @Test
        void preservesNanos() {
            var bytes = RetryInfoCodec.encodeRetryInfo(UNAVAILABLE, "throttled", Duration.ofMillis(250));
            assertThat(RetryInfoCodec.decodeRetryDelay(bytes)).isEqualTo(Duration.ofMillis(250));
        }

        @DisplayName("an absent trailer decodes to null")
        @Test
        void absentIsNull() {
            assertThat(RetryInfoCodec.decodeRetryDelay((String) null)).isNull();
        }

        @DisplayName("a malformed base64 trailer decodes to null")
        @Test
        void malformedBase64IsNull() {
            assertThat(RetryInfoCodec.decodeRetryDelay("not valid base64 @@@")).isNull();
        }

        @DisplayName("a status carrying no RetryInfo detail decodes to null")
        @Test
        void noRetryInfoDetailIsNull() {
            var status = com.google.rpc.Status.newBuilder().setCode(UNAVAILABLE).setMessage("x").build();
            assertThat(RetryInfoCodec.decodeRetryDelay(status.toByteArray())).isNull();
        }
    }

    @DisplayName("the decision maps a delay of zero cleanly (a boundary a zero-hint would hit)")
    @Test
    void zeroDelayDecision() {
        var decision = ArmeriaRetryRules.decideGrpc(UNAVAILABLE, 0L, null, false, true);
        // A zero pushback is not a positive hint, so the default backoff is used.
        assertThat(decision).isEqualTo(new Decision(Kind.RETRY_DEFAULT, null));
    }
}
