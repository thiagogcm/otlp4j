package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.components.internal.MatchRule;
import dev.nthings.otlp4j.components.internal.MatchSpec;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigBinder;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The factory for the built-in filter processor, type `filter`, stability `ALPHA`.
///
/// It supports every signal, since a filter rule reads only the routing-metadata and client-metadata
/// axes (or, through the escape hatch, the domain), none of which is signal-specific. It implements
/// [ConfigBinder] so a declarative loader binds an `include` and an `exclude` sub-map, each carrying up
/// to four condition maps (`resource_attribute`, `resource_attribute_regex`, `header`, `header_regex`)
/// onto one bound rule, per [MatchRule#fromConfig].
public final class FilterProcessorFactory implements ProcessorFactory<FilterConfig>, ConfigBinder<FilterConfig> {

    /// The component type name.
    public static final String TYPE = "filter";

    /// Creates a filter processor factory.
    public FilterProcessorFactory() {
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.PROCESSOR;
    }

    @Override
    public StabilityLevel stability() {
        return StabilityLevel.ALPHA;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public FilterConfig defaultConfig() {
        return FilterConfig.defaults();
    }

    @Override
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, FilterConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        Processor<TelemetryPayload> processor = new FilterProcessor(
                ctx.id(), signal, config.include(), config.exclude(), payloadConsumer(next));
        return castProcessor(processor);
    }

    @Override
    public FilterConfig bind(ConfigNode node, FilterConfig defaults) {
        return new FilterConfig(
                bindSpec(node, "include", defaults.include()), bindSpec(node, "exclude", defaults.exclude()));
    }

    /// Binds the sub-map under `key` (`include` or `exclude`) into a spec carrying at most the one rule
    /// the sub-map's condition maps describe, falling back to `fallback` when the key is absent or
    /// describes no condition at all.
    private static MatchSpec bindSpec(ConfigNode node, String key, MatchSpec fallback) {
        var child = node.child(key);
        if (child.isEmpty()) {
            return fallback;
        }
        return MatchRule.fromConfig(child.get()).map(MatchSpec::of).orElse(fallback);
    }

    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> payloadConsumer(SignalConsumer<?> next) {
        return (SignalConsumer<TelemetryPayload>) next;
    }

    @SuppressWarnings("unchecked")
    private static <T extends TelemetryPayload> Processor<T> castProcessor(Processor<TelemetryPayload> processor) {
        return (Processor<T>) (Processor<?>) processor;
    }
}
