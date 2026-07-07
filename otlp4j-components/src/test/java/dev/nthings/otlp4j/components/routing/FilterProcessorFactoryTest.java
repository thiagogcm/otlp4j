package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FilterProcessorFactoryTest {

    private final FilterProcessorFactory factory = new FilterProcessorFactory();

    @Test
    void metadataIsHonest() {
        assertThat(factory.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(factory.stability()).isEqualTo(StabilityLevel.ALPHA);
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

    @Test
    void bindReadsIncludeAndExclude() {
        var node = ConfigNode.of(Map.of(
                "include", Map.of("resource_attribute", Map.of("service.name", "checkout")),
                "exclude", Map.of("header", Map.of("x-drop", "yes"))));

        var config = factory.bind(node, factory.defaultConfig());

        assertThat(config.include().matches(traces(1), new TestConsumeContext())).isTrue();
        assertThat(config.exclude().matches(traces(1), TestConsumeContext.withHeader("x-drop", "yes"))).isTrue();
        assertThat(config.exclude().matches(traces(1), new TestConsumeContext())).isFalse();
        assertThat(node.unreadKeys()).isEmpty();
    }

    @Test
    void bindFallsBackToDefaultsForAbsentKeys() {
        var config = factory.bind(ConfigNode.empty(), factory.defaultConfig());
        assertThat(config).isEqualTo(factory.defaultConfig());
    }

    @Test
    void bindReportsUnknownKeyAsUnread() {
        var node = ConfigNode.of(Map.of(
                "include", Map.of("resource_attribute", Map.of("service.name", "checkout")), "bogus", "x"));
        factory.bind(node, factory.defaultConfig());
        assertThat(node.unreadKeys()).containsExactly("bogus");
    }

    @SuppressWarnings("unchecked")
    private Processor<TelemetryPayload> create(SignalDescriptor<?> signal) {
        var ctx = new CreateContext(ComponentId.parse("filter"), baseContext());
        var typed = (SignalDescriptor<TelemetryPayload>) signal;
        SignalConsumer<TelemetryPayload> next = new RecordingConsumer();
        return factory.create(ctx, factory.defaultConfig(), typed, next);
    }
}
