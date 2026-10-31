package dev.nthings.otlp4j.components.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchProcessorConfigTest {

    private static final BatchPolicy BATCH = new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of());
    private static final DeliveryQueuePolicy DELIVERY = new DeliveryQueuePolicy(1024, 1, OverflowPolicy.FAIL);

    @Test
    void rejectsNullDegradedWindow() {
        assertThatThrownBy(() -> config(null)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("degradedWindow");
    }

    @Test
    void rejectsNullWrappedPolicies() {
        assertThatThrownBy(() -> new BatchProcessorConfig(null, DELIVERY, Duration.ZERO, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("batch");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, null, Duration.ZERO, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("delivery");
    }

    @Test
    void rejectsNegativeDegradedWindow() {
        assertThatThrownBy(() -> config(Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("degradedWindow");
    }

    @Test
    void rejectsNonPositiveAdmissionLimits() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, DELIVERY, Duration.ZERO, 0, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPendingSubmissions");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, DELIVERY, Duration.ZERO, 1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPendingBytes");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, DELIVERY, Duration.ZERO, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPartitions");
    }

    @Test
    void rejectsANonPositivePendingItemLimitAndANullSizer() {
        assertThatThrownBy(() -> new BatchProcessorConfig(
                        BATCH, DELIVERY, Duration.ZERO, 1, 1, 1, 0, PayloadSizer.reported()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxPendingItems");
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, DELIVERY, Duration.ZERO, 1, 1, 1, 1, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sizer");
    }

    @Test
    void aHardByteCapNeedsASizerThatCanWeighTheBatchesTheProcessorBuilds() {
        var capped = new BatchPolicy(8192, 0, 4096, Duration.ofSeconds(1), List.of());

        assertThatThrownBy(() -> new BatchProcessorConfig(capped, DELIVERY, Duration.ZERO, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sendBatchMaxBytes");
        assertThatThrownBy(() -> new BatchProcessorConfig(
                        capped, DELIVERY, Duration.ZERO, 1, 1, 1, 1, PayloadSizer.reported()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sendBatchMaxBytes");

        PayloadSizer total = payload -> 100L * payload.itemCount();
        assertThat(new BatchProcessorConfig(capped, DELIVERY, Duration.ZERO, 1, 1, 1, 1, total).sizer())
                .isSameAs(total);
    }

    private static BatchProcessorConfig config(Duration degradedWindow) {
        return new BatchProcessorConfig(BATCH, DELIVERY, degradedWindow, 1, 1, 1);
    }

    @Test
    void aHardCapNeedsAQueueThatCanHoldOneFlush() {
        var byteCap = new BatchPolicy(8192, 0, 1024, Duration.ofSeconds(1), List.of());
        var itemCap = new BatchPolicy(8, 8, 0, Duration.ofSeconds(1), List.of());
        var noCap = new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of());
        var small = new DeliveryQueuePolicy(63, 1, OverflowPolicy.FAIL);
        var enough = new DeliveryQueuePolicy(64, 1, OverflowPolicy.FAIL);

        assertThatThrownBy(() -> config(byteCap, small)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queueSize");
        assertThatThrownBy(() -> config(itemCap, small)).isInstanceOf(IllegalArgumentException.class);
        assertThat(config(byteCap, enough).delivery()).isEqualTo(enough);
        assertThat(config(noCap, small).delivery()).isEqualTo(small);
    }

    private static BatchProcessorConfig config(BatchPolicy batch, DeliveryQueuePolicy delivery) {
        return new BatchProcessorConfig(
                batch,
                delivery,
                Duration.ofSeconds(5),
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_ITEMS,
                payload -> 1L);
    }
}
