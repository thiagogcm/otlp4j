package dev.nthings.otlp4j.components.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchProcessorConfigTest {

    private static final BatchPolicy BATCH = new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of());

    @Test
    void rejectsANullBatchPolicy() {
        assertThatThrownBy(() -> new BatchProcessorConfig(null, 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("batch");
    }

    @Test
    void rejectsFewerThanOneConsumer() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, 0, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("numConsumers");
    }

    @Test
    void rejectsNonPositiveAdmissionLimits() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, 1, 0, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPendingSubmissions");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, 1, 1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPendingBytes");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, 1, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPartitions");
    }

    @Test
    void rejectsANonPositivePendingItemLimitAndANullSizer() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, 1, 1, 1, 1, 0, PayloadSizer.reported()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPendingItems");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, 1, 1, 1, 1, 1, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sizer");
    }

    @Test
    void aHardByteCapNeedsASizerThatCanWeighTheBatchesTheProcessorBuilds() {
        var capped = new BatchPolicy(8192, 0, 4096, Duration.ofSeconds(1), List.of());

        assertThatThrownBy(() -> new BatchProcessorConfig(capped, 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sendBatchMaxBytes");
        assertThatThrownBy(() -> new BatchProcessorConfig(capped, 1, 1, 1, 1, 1, PayloadSizer.reported()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sendBatchMaxBytes");

        PayloadSizer total = payload -> 100L * payload.itemCount();
        assertThat(new BatchProcessorConfig(capped, 1, 1, 1, 1, 1, total).sizer()).isSameAs(total);
    }

    @Test
    void aHardItemCapNeedsNoSizerAndNoQueueSizing() {
        var itemCap = new BatchPolicy(8, 8, 0, Duration.ofSeconds(1), List.of());

        assertThat(new BatchProcessorConfig(itemCap, 1, 1, 1, 1).numConsumers()).isEqualTo(1);
    }

    @Test
    void defaultsUseASingleConsumer() {
        assertThat(BatchProcessorConfig.defaults().numConsumers()).isEqualTo(1);
    }
}
