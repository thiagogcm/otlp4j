package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import org.junit.jupiter.api.Test;

class FilterProcessorFactoryTest {

    private final FilterProcessorFactory factory = new FilterProcessorFactory();

    @Test
    void metadataIsHonest() {
        assertThat(factory.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(factory.type()).isEqualTo("filter");
        assertThat(factory.defaultConfig()).isEqualTo(FilterConfig.defaults());
        assertThat(factory.defaultConfig()).isEqualTo(factory.defaultConfig());
    }

    @Test
    void createsAProcessorForEverySignal() {
        assertThat(create(SignalDescriptor.TRACES)).isNotNull();
        assertThat(create(SignalDescriptor.METRICS)).isNotNull();
        assertThat(create(SignalDescriptor.LOGS)).isNotNull();
        assertThat(create(SignalDescriptor.PROFILES)).isNotNull();
    }

    @SuppressWarnings("unchecked")
    private Processor<TelemetryPayload> create(SignalDescriptor<?> signal) {
        var ctx = new CreateContext(ComponentId.parse("filter"), baseContext());
        var typed = (SignalDescriptor<TelemetryPayload>) signal;
        SignalConsumer<TelemetryPayload> next = new RecordingConsumer();
        return factory.create(ctx, factory.defaultConfig(), typed, next);
    }
}
