package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
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
        var result = ConsumeResult.<TracesData>partial(2L, null);
        var r = (ConsumeResult.Partial<TracesData>) result;
        assertThat(r.message()).isEmpty();
    }

    @DisplayName("retryable() normalises null message, leaves cause null, and is retryable")
    @Test
    void retryableNormalisesNullMessage() {
        var result = ConsumeResult.<TracesData>retryable(null);
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.message()).isEmpty();
        assertThat(r.cause()).isNull();
        assertThat(r.retryable()).isTrue();
    }

    @DisplayName("retryable() yields a retryable Rejected with no cause")
    @Test
    void retryableHasNoCause() {
        var result = ConsumeResult.<TracesData>retryable("queue full");
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.message()).isEqualTo("queue full");
        assertThat(r.cause()).isNull();
        assertThat(r.retryable()).isTrue();
    }

    @DisplayName("retryable() can carry a diagnostic cause and stay retryable")
    @Test
    void retryableCarriesCauseAndStaysRetryable() {
        var cause = new IOException("downstream briefly down");
        var result = ConsumeResult.<TracesData>retryable("downstream down", cause);
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.retryable()).isTrue();
        assertThat(r.cause()).isSameAs(cause);
    }

    @DisplayName("permanent() carries the given cause and is not retryable")
    @Test
    void permanentCarriesCause() {
        var cause = new IllegalArgumentException("invalid tenant");
        var result = ConsumeResult.<TracesData>permanent("rejected by policy", cause);
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.message()).isEqualTo("rejected by policy");
        assertThat(r.cause()).isSameAs(cause);
        assertThat(r.retryable()).isFalse();
    }

    @DisplayName("permanent() needs no cause to express a permanent rejection")
    @Test
    void permanentWithoutCause() {
        var result = ConsumeResult.<TracesData>permanent("policy rejected batch");
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.retryable()).isFalse();
        assertThat(r.cause()).isNull();
    }

    @DisplayName("fanOutMerge of all Accepted yields Accepted")
    @Test
    void fanOutMergeOfAllAcceptedIsAccepted() {
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.accepted(),
                ConsumeResult.accepted()));
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fanOutMerge takes the max rejected count, not the sum")
    @Test
    void fanOutMergeUsesMaxRejection() {
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(10L, "a"),
                ConsumeResult.partial(7L, "b")));
        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial<TracesData>) result;
        assertThat(p.rejectedItems()).isEqualTo(10L); // MAX not sum
        assertThat(p.message()).contains("a").contains("b");
    }

    @DisplayName("fanOutMerge lets Rejected dominate Partial and Accepted")
    @Test
    void fanOutMergeRejectionDominatesPartial() {
        var rejected = ConsumeResult.<TracesData>retryable("boom");
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(5L, "p"),
                rejected));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("fanOutMerge is permanent if any rejecting peer is permanent")
    @Test
    void fanOutMergePermanentDominatesRetryable() {
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.retryable("transient"),
                ConsumeResult.permanent("policy")));
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.retryable()).isFalse();
    }

    @DisplayName("fanOutMerge stays retryable when every rejecting peer is retryable, keeping the first cause")
    @Test
    void fanOutMergeAllRetryableStaysRetryable() {
        var cause = new IOException("down");
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.retryable("a"),
                ConsumeResult.retryable("b", cause)));
        var r = (ConsumeResult.Rejected<TracesData>) result;
        assertThat(r.retryable()).isTrue();
        assertThat(r.cause()).isSameAs(cause);
    }

    @DisplayName("fanOutMerge of an empty list yields Accepted")
    @Test
    void fanOutMergeEmptyIsAccepted() {
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of());
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fanOutMerge of empty-message rejections keeps message empty")
    @Test
    void fanOutMergeKeepsFirstRejectionAndOmitsEmptyMessages() {
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.retryable(""),
                ConsumeResult.retryable("")));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        assertThat(((ConsumeResult.Rejected<TracesData>) result).message()).isEmpty();
    }

    @DisplayName("fanOutMerge keeps Partial count with an empty message")
    @Test
    void fanOutMergePartialWithoutMessageReportsCountOnly() {
        var result = ConsumeResult.<TracesData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(4L, "")));
        var p = (ConsumeResult.Partial<TracesData>) result;
        assertThat(p.rejectedItems()).isEqualTo(4L);
        assertThat(p.message()).isEmpty();
    }
}
