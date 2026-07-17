package dev.nthings.otlp4j.components.batch;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import org.junit.jupiter.api.Test;

class BatchProcessorFactoryTest {

    private final BatchProcessorFactory factory = new BatchProcessorFactory();

    @Test
    void metadataIsHonest() {
        assertThat(factory.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(factory.type()).isEqualTo("batch");
        assertThat(factory.defaultConfig()).isEqualTo(BatchProcessorConfig.defaults());
    }

    @Test
    void supportsEverySignalExceptProfiles() {
        assertThat(factory.supportsSignal(SignalDescriptor.TRACES)).isTrue();
        assertThat(factory.supportsSignal(SignalDescriptor.METRICS)).isTrue();
        assertThat(factory.supportsSignal(SignalDescriptor.LOGS)).isTrue();
        assertThat(factory.supportsSignal(SignalDescriptor.PROFILES)).isFalse();
    }
}
