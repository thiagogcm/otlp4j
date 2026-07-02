package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ConsumeResult")
class ConsumeResultTest {

    @DisplayName("accepted() returns a shared singleton instance")
    @Test
    void acceptedIsShared() {
        var a = ConsumeResult.accepted();
        var b = ConsumeResult.accepted();
        assertThat(a).isSameAs(b);
    }

    @DisplayName("partial() rejects a non-positive rejected count")
    @Test
    void partialRejectsNonPositiveCount() {
        assertThatThrownBy(() -> ConsumeResult.partial(0L, "nope"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConsumeResult.partial(-1L, "nope"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("partial() normalises a null message to empty")
    @Test
    void partialNormalisesNullMessage() {
        var result = ConsumeResult.partial(2L, null);
        var r = (ConsumeResult.Partial) result;
        assertThat(r.message()).isEmpty();
    }

    @DisplayName("retryable() normalises null message, leaves cause null, and is retryable")
    @Test
    void retryableNormalisesNullMessage() {
        var result = ConsumeResult.retryable(null);
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.message()).isEmpty();
        assertThat(r.cause()).isNull();
        assertThat(r.retryable()).isTrue();
    }

    @DisplayName("retryable() yields a retryable Rejected with no cause")
    @Test
    void retryableHasNoCause() {
        var result = ConsumeResult.retryable("queue full");
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.message()).isEqualTo("queue full");
        assertThat(r.cause()).isNull();
        assertThat(r.retryable()).isTrue();
    }

    @DisplayName("retryable() can carry a diagnostic cause and stay retryable")
    @Test
    void retryableCarriesCauseAndStaysRetryable() {
        var cause = new IOException("downstream briefly down");
        var result = ConsumeResult.retryable("downstream down", cause);
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.retryable()).isTrue();
        assertThat(r.cause()).isSameAs(cause);
    }

    @DisplayName("permanent() carries the given cause and is not retryable")
    @Test
    void permanentCarriesCause() {
        var cause = new IllegalArgumentException("invalid tenant");
        var result = ConsumeResult.permanent("rejected by policy", cause);
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.message()).isEqualTo("rejected by policy");
        assertThat(r.cause()).isSameAs(cause);
        assertThat(r.retryable()).isFalse();
    }

    @DisplayName("permanent() needs no cause to express a permanent rejection")
    @Test
    void permanentWithoutCause() {
        var result = ConsumeResult.permanent("policy rejected batch");
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.retryable()).isFalse();
        assertThat(r.cause()).isNull();
    }

    @DisplayName("fanOutMerge of all Accepted yields Accepted")
    @Test
    void fanOutMergeOfAllAcceptedIsAccepted() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.accepted(),
                ConsumeResult.accepted()));
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fanOutMerge takes the max rejected count, not the sum")
    @Test
    void fanOutMergeUsesMaxRejection() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(10L, "a"),
                ConsumeResult.partial(7L, "b")));
        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial) result;
        assertThat(p.rejectedItems()).isEqualTo(10L); // MAX not sum
        assertThat(p.message()).contains("a").contains("b");
    }

    @DisplayName("fanOutMerge lets Rejected dominate Partial and Accepted")
    @Test
    void fanOutMergeRejectionDominatesPartial() {
        var rejected = ConsumeResult.retryable("boom");
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(5L, "p"),
                rejected));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("fanOutMerge is permanent if any rejecting peer is permanent")
    @Test
    void fanOutMergePermanentDominatesRetryable() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.retryable("transient"),
                ConsumeResult.permanent("policy")));
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.retryable()).isFalse();
    }

    @DisplayName("fanOutMerge stays retryable when every rejecting peer is retryable, keeping the first cause")
    @Test
    void fanOutMergeAllRetryableStaysRetryable() {
        var cause = new IOException("down");
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.retryable("a"),
                ConsumeResult.retryable("b", cause)));
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.retryable()).isTrue();
        assertThat(r.cause()).isSameAs(cause);
    }

    @DisplayName("fanOutMerge of an empty list yields Accepted")
    @Test
    void fanOutMergeEmptyIsAccepted() {
        var result = ConsumeResult.fanOutMerge(List.of());
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fanOutMerge of empty-message rejections keeps message empty")
    @Test
    void fanOutMergeKeepsFirstRejectionAndOmitsEmptyMessages() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.retryable(""),
                ConsumeResult.retryable("")));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        assertThat(((ConsumeResult.Rejected) result).message()).isEmpty();
    }

    @DisplayName("fanOutMerge keeps Partial count with an empty message")
    @Test
    void fanOutMergePartialWithoutMessageReportsCountOnly() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(4L, "")));
        var p = (ConsumeResult.Partial) result;
        assertThat(p.rejectedItems()).isEqualTo(4L);
        assertThat(p.message()).isEmpty();
    }

    @DisplayName("throttled() yields a retryable Rejected carrying the retry-after hint")
    @Test
    void throttledCarriesRetryAfter() {
        var cause = new IOException("busy");
        var plain = (ConsumeResult.Rejected) ConsumeResult.throttled(Duration.ofSeconds(2), "at capacity");
        assertThat(plain.retryable()).isTrue();
        assertThat(plain.retryAfter()).isEqualTo(Duration.ofSeconds(2));
        assertThat(plain.cause()).isNull();

        var withCause =
                (ConsumeResult.Rejected) ConsumeResult.throttled(Duration.ofMillis(250), "at capacity", cause);
        assertThat(withCause.retryable()).isTrue();
        assertThat(withCause.retryAfter()).isEqualTo(Duration.ofMillis(250));
        assertThat(withCause.cause()).isSameAs(cause);
    }

    @DisplayName("retryable()/permanent() factories carry no retry-after hint")
    @Test
    void plainFactoriesCarryNoRetryAfter() {
        assertThat(((ConsumeResult.Rejected) ConsumeResult.retryable("r")).retryAfter()).isNull();
        assertThat(((ConsumeResult.Rejected) ConsumeResult.permanent("p")).retryAfter()).isNull();
    }

    @DisplayName("Rejected rejects a zero, negative, or permanent retry-after hint")
    @Test
    void rejectedValidatesRetryAfter() {
        assertThatThrownBy(() -> ConsumeResult.throttled(Duration.ZERO, "x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConsumeResult.throttled(Duration.ofSeconds(-1), "x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConsumeResult.Rejected(false, "x", null, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryable");
    }

    @DisplayName("fanOutMerge keeps the longest retry-after across retryable rejections")
    @Test
    void fanOutMergeKeepsLongestRetryAfter() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.throttled(Duration.ofSeconds(1), "a"),
                ConsumeResult.retryable("b"),
                ConsumeResult.throttled(Duration.ofSeconds(5), "c")));
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.retryable()).isTrue();
        assertThat(r.retryAfter()).isEqualTo(Duration.ofSeconds(5));
    }

    @DisplayName("fanOutMerge drops the retry-after hint when the merge is permanent")
    @Test
    void fanOutMergeDropsRetryAfterWhenPermanent() {
        var result = ConsumeResult.fanOutMerge(List.of(
                ConsumeResult.throttled(Duration.ofSeconds(5), "a"),
                ConsumeResult.permanent("b")));
        var r = (ConsumeResult.Rejected) result;
        assertThat(r.retryable()).isFalse();
        assertThat(r.retryAfter()).isNull();
    }
}
