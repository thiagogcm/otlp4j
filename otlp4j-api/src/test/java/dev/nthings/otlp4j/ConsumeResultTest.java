package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ConsumeResult")
class ConsumeResultTest {

    @DisplayName("accepted() returns a shared singleton instance")
    @Test
    void acceptedIsShared() {
        ConsumeResult<TraceData> a = ConsumeResult.accepted();
        ConsumeResult<TraceData> b = ConsumeResult.accepted();
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
        ConsumeResult<TraceData> result = ConsumeResult.partial(2L, null);
        var r = (ConsumeResult.Partial<TraceData>) result;
        assertThat(r.message()).isEmpty();
    }

    @DisplayName("rejected() normalises null message and leaves cause null")
    @Test
    void rejectedNormalisesNullMessage() {
        ConsumeResult<TraceData> result = ConsumeResult.rejected(null);
        var r = (ConsumeResult.Rejected<TraceData>) result;
        assertThat(r.message()).isEmpty();
        assertThat(r.cause()).isNull();
    }

    @DisplayName("retryableRejected() yields a Rejected with no cause (retryable)")
    @Test
    void retryableRejectedHasNoCause() {
        ConsumeResult<TraceData> result = ConsumeResult.retryableRejected("queue full");
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected<TraceData>) result;
        assertThat(r.message()).isEqualTo("queue full");
        assertThat(r.cause()).isNull();
    }

    @DisplayName("permanentRejected() carries the given cause (non-retryable)")
    @Test
    void permanentRejectedCarriesCause() {
        var cause = new IllegalArgumentException("invalid tenant");
        ConsumeResult<TraceData> result = ConsumeResult.permanentRejected("rejected by policy", cause);
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected<TraceData>) result;
        assertThat(r.message()).isEqualTo("rejected by policy");
        assertThat(r.cause()).isSameAs(cause);
    }

    @DisplayName("permanentRejected() requires a non-null cause")
    @Test
    void permanentRejectedRejectsNullCause() {
        assertThatThrownBy(() -> ConsumeResult.permanentRejected("nope", null))
                .isInstanceOf(NullPointerException.class);
    }

    @DisplayName("fanOutMerge of all Accepted yields Accepted")
    @Test
    void fanOutMergeOfAllAcceptedIsAccepted() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.accepted(),
                ConsumeResult.accepted()));
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fanOutMerge takes the max rejected count, not the sum")
    @Test
    void fanOutMergeUsesMaxRejection() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(10L, "a"),
                ConsumeResult.partial(7L, "b")));
        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial<TraceData>) result;
        assertThat(p.rejectedItems()).isEqualTo(10L); // MAX not sum
        assertThat(p.message()).contains("a").contains("b");
    }

    @DisplayName("fanOutMerge lets Rejected dominate Partial and Accepted")
    @Test
    void fanOutMergeRejectionDominatesPartial() {
        var rejected = ConsumeResult.<TraceData>rejected("boom");
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(5L, "p"),
                rejected));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("fanOutMerge of an empty list yields Accepted")
    @Test
    void fanOutMergeEmptyIsAccepted() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of());
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fanOutMerge of empty-message rejections keeps message empty")
    @Test
    void fanOutMergeKeepsFirstRejectionAndOmitsEmptyMessages() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.rejected(""),
                ConsumeResult.rejected("")));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        assertThat(((ConsumeResult.Rejected<TraceData>) result).message()).isEmpty();
    }

    @DisplayName("fanOutMerge keeps Partial count with an empty message")
    @Test
    void fanOutMergePartialWithoutMessageReportsCountOnly() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(4L, "")));
        var p = (ConsumeResult.Partial<TraceData>) result;
        assertThat(p.rejectedItems()).isEqualTo(4L);
        assertThat(p.message()).isEmpty();
    }
}
