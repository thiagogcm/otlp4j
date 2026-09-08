package dev.nthings.otlp4j.core.result;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Contract tests for the bounded flush outcome and its aggregation semantics.
class FlushReportTest {

    @Test
    @DisplayName("empty and warning-only reports are successful")
    void successSemantics() {
        assertThat(FlushReport.empty().isSuccess()).isTrue();
        assertThat(new FlushReport(0, 0, 0, null).isSuccess()).isTrue();
        assertThat(new FlushReport(1, 0, 0, null).isSuccess()).isFalse();
        assertThat(new FlushReport(0, 1, 0, null).isSuccess()).isFalse();
        assertThat(new FlushReport(0, 0, 1, null).isSuccess()).isFalse();
    }

    @Test
    @DisplayName("plus sums counters and retains the first failure")
    void plus() {
        var firstFailure = new IllegalStateException("first");
        var secondFailure = new IllegalArgumentException("second");
        var first = new FlushReport(1, 2, 3, firstFailure);
        var second = new FlushReport(4, 5, 6, secondFailure);

        assertThat(first.plus(second))
                .isEqualTo(new FlushReport(5, 7, 9, firstFailure));
        assertThat(FlushReport.empty().plus(second)).isEqualTo(second);
        assertThatThrownBy(() -> first.plus(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("flush counters reject negative values")
    void rejectsNegativeCounters() {
        assertThatThrownBy(() -> new FlushReport(-1, 0, 0, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlushReport(0, -1, 0, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlushReport(0, 0, -1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
