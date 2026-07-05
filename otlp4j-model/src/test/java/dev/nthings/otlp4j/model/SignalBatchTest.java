package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for [SignalBatch]: the null-check invariants and the `itemCount` delegation to the carried
/// signal that the transport integration paths do not exercise directly.
@DisplayName("SignalBatch")
class SignalBatchTest {

    @DisplayName("itemCount delegates to the carried signal")
    @Test
    void itemCountDelegates() {
        var traces = TracesData.of(
                Resource.EMPTY,
                InstrumentationScope.of("s"),
                List.of(Span.builder().name("a").build(), Span.builder().name("b").build()));
        var batch = new SignalBatch<>(Signal.TRACES, traces);
        assertThat(batch.itemCount()).isEqualTo(2L);
    }

    @DisplayName("rejects a null signal")
    @Test
    void rejectsNullSignal() {
        assertThatNullPointerException()
                .isThrownBy(() -> new SignalBatch<>(null, new TracesData(List.of())))
                .withMessage("signal");
    }

    @DisplayName("rejects a null batch")
    @Test
    void rejectsNullBatch() {
        assertThatNullPointerException()
                .isThrownBy(() -> new SignalBatch<>(Signal.TRACES, null))
                .withMessage("batch");
    }
}
