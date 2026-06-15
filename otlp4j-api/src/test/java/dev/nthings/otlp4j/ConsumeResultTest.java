package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConsumeResultTest {

    @Test
    void acceptedIsShared() {
        ConsumeResult<TraceData> a = ConsumeResult.accepted();
        ConsumeResult<TraceData> b = ConsumeResult.accepted();
        assertThat(a).isSameAs(b);
    }

    @Test
    void partialRejectsNonPositiveCount() {
        assertThatThrownBy(() -> ConsumeResult.partial(0L, "nope"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConsumeResult.partial(-1L, "nope"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void partialNormalisesNullMessage() {
        ConsumeResult<TraceData> result = ConsumeResult.partial(2L, null);
        var r = (ConsumeResult.Partial<TraceData>) result;
        assertThat(r.message()).isEmpty();
    }

    @Test
    void rejectedNormalisesNullMessage() {
        ConsumeResult<TraceData> result = ConsumeResult.rejected(null);
        var r = (ConsumeResult.Rejected<TraceData>) result;
        assertThat(r.message()).isEmpty();
        assertThat(r.cause()).isNull();
    }

    @Test
    void fanOutMergeOfAllAcceptedIsAccepted() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.accepted(),
                ConsumeResult.accepted()));
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

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

    @Test
    void fanOutMergeRejectionDominatesPartial() {
        var rejected = ConsumeResult.<TraceData>rejected("boom");
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.accepted(),
                ConsumeResult.partial(5L, "p"),
                rejected));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @Test
    void fanOutMergeEmptyIsAccepted() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of());
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @Test
    void fanOutMergeKeepsFirstRejectionAndOmitsEmptyMessages() {
        var result = ConsumeResult.<TraceData>fanOutMerge(List.of(
                ConsumeResult.rejected(""),
                ConsumeResult.rejected("")));
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        assertThat(((ConsumeResult.Rejected<TraceData>) result).message()).isEmpty();
    }

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
