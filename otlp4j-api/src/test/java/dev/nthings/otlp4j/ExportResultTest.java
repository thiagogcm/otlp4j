package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.pipeline.ExportResult;
import org.junit.jupiter.api.Test;

/// Unit tests for the `ExportResult` value type — the constructor guard, the factory semantics,
/// `isFullSuccess`, and the full `and` branch matrix including every message-join sub-branch.
class ExportResultTest {

    @Test
    void rejectsANegativeRejectedCount() {
        assertThatThrownBy(() -> new ExportResult(-1, "bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rejectedCount must be >= 0");
        assertThatThrownBy(() -> new ExportResult(Long.MIN_VALUE, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rejectedCount must be >= 0");
    }

    @Test
    void coalescesANullMessageToEmpty() {
        assertThat(new ExportResult(0, null).message()).isEmpty();
    }

    @Test
    void successIsFullSuccess() {
        assertThat(ExportResult.success().isFullSuccess()).isTrue();
        assertThat(ExportResult.success().rejectedCount()).isZero();
        assertThat(ExportResult.success().message()).isEmpty();
    }

    @Test
    void partialSuccessIsNotFullSuccess() {
        var partial = ExportResult.partialSuccess(3, "3 rejected");
        assertThat(partial.isFullSuccess()).isFalse();
        assertThat(partial.rejectedCount()).isEqualTo(3);
        assertThat(partial.message()).isEqualTo("3 rejected");
    }

    @Test
    void rejectedHasZeroCountAndANonEmptyMessage() {
        var rejected = ExportResult.rejected("whole batch refused");
        assertThat(rejected.rejectedCount())
                .as("rejected() keeps rejectedCount at 0 per the OTLP spec")
                .isZero();
        assertThat(rejected.message()).isEqualTo("whole batch refused");
        assertThat(rejected.isFullSuccess())
                .as("a non-empty message alone makes a result not a full success")
                .isFalse();
    }

    @Test
    void aResultWithACountButNoMessageIsNotFullSuccess() {
        assertThat(new ExportResult(2, "").isFullSuccess()).isFalse();
    }

    @Test
    void andOverSuccessAndSuccessIsSuccess() {
        var combined = ExportResult.success().and(ExportResult.success());
        assertThat(combined.isFullSuccess()).isTrue();
    }

    @Test
    void andIsIdentityOverFullSuccessOnEitherSide() {
        var partial = ExportResult.partialSuccess(1, "one rejected");
        assertThat(ExportResult.success().and(partial)).isEqualTo(partial);
        assertThat(partial.and(ExportResult.success())).isEqualTo(partial);
    }

    @Test
    void andCombinesCountsAndJoinsTwoNonEmptyMessages() {
        var combined = ExportResult.partialSuccess(2, "2 spans malformed")
                .and(ExportResult.partialSuccess(3, "3 spans rejected"));

        assertThat(combined.rejectedCount()).isEqualTo(5);
        assertThat(combined.message()).isEqualTo("2 spans malformed; 3 spans rejected");
    }

    @Test
    void andUsesTheOtherMessageWhenThisMessageIsEmpty() {
        var combined = new ExportResult(2, "").and(ExportResult.partialSuccess(3, "3 rejected"));

        assertThat(combined.rejectedCount()).isEqualTo(5);
        assertThat(combined.message())
                .as("this.message empty -> the combined message is other's")
                .isEqualTo("3 rejected");
    }

    @Test
    void andUsesThisMessageWhenTheOtherMessageIsEmpty() {
        var combined = ExportResult.partialSuccess(2, "2 rejected").and(new ExportResult(3, ""));

        assertThat(combined.rejectedCount()).isEqualTo(5);
        assertThat(combined.message())
                .as("other.message empty -> the combined message is this's")
                .isEqualTo("2 rejected");
    }

    @Test
    void andWithBothMessagesEmptyButNonZeroCountsSumsCountsAndKeepsAnEmptyMessage() {
        var combined = new ExportResult(2, "").and(new ExportResult(3, ""));

        assertThat(combined.rejectedCount()).isEqualTo(5);
        assertThat(combined.message())
                .as("both messages empty -> the combined message stays empty")
                .isEmpty();
        assertThat(combined.isFullSuccess())
                .as("a non-zero combined count is still not a full success")
                .isFalse();
    }
}
