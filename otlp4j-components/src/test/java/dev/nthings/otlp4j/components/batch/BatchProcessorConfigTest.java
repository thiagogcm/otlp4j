package dev.nthings.otlp4j.components.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchProcessorConfigTest {

    private static final BatchPolicy BATCH = new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of());
    private static final DeliveryQueuePolicy DELIVERY = new DeliveryQueuePolicy(1024, 1, OverflowPolicy.BLOCK, false);

    @Test
    void defaultsCarryTheDocumentedValues() {
        var config = BatchProcessorConfig.defaults();
        assertThat(config.batch().sendBatchSize()).isEqualTo(8192);
        assertThat(config.batch().sendBatchMaxSize()).isZero();
        assertThat(config.batch().sendBatchMaxBytes()).isZero();
        assertThat(config.batch().timeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.batch().metadataKeys()).isEmpty();
        assertThat(config.delivery().queueSize()).isEqualTo(1024);
        assertThat(config.delivery().numConsumers()).isEqualTo(1);
        assertThat(config.delivery().overflow()).isEqualTo(OverflowPolicy.BLOCK);
        assertThat(config.delivery().waitForResult()).isFalse();
        assertThat(config.degradedWindow()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsNullBatch() {
        assertThatThrownBy(() -> new BatchProcessorConfig(null, DELIVERY, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch policy");
    }

    @Test
    void rejectsNullDelivery() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, null, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("delivery policy");
    }

    @Test
    void rejectsNullDegradedWindow() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, DELIVERY, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("degradedWindow");
    }

    @Test
    void rejectsNegativeDegradedWindow() {
        assertThatThrownBy(() -> new BatchProcessorConfig(BATCH, DELIVERY, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("degradedWindow");
    }
}
