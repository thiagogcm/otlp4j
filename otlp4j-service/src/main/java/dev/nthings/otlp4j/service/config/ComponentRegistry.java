package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/// The lookup over a [GatewayConfig] that resolves a [ComponentId] to its entry and answers the
/// existence and signal-support queries the validator asks. This is the declarative replacement for
/// the reflective terminal-shape discovery and lifecycle-collection the fluent pipeline performed at
/// build time: instead of probing a built object with `instanceof`, the registry reads the declared
/// factory metadata.
///
/// A component can be referenced by a pipeline in a role its kind does not match (a receiver id in an
/// exporter list, say). The registry keeps the five kinds in separate maps, so a role query is a map
/// lookup, and signal support is answered by consulting the factory's optional [SignalSupport]
/// capability, defaulting to full support when the factory does not declare one.
public final class ComponentRegistry {

    private final GatewayConfig config;

    /// Wraps `config` as a registry.
    ///
    /// @param config the gateway config to resolve against
    public ComponentRegistry(GatewayConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must be non-null");
        }
        this.config = config;
    }

    /// The wrapped config.
    ///
    /// @return the config
    public GatewayConfig config() {
        return config;
    }

    /// Whether a receiver is registered under `id`.
    ///
    /// @param id the component id
    /// @return true if a receiver is registered
    public boolean hasReceiver(ComponentId id) {
        return config.receivers().containsKey(id);
    }

    /// Whether a processor is registered under `id`.
    ///
    /// @param id the component id
    /// @return true if a processor is registered
    public boolean hasProcessor(ComponentId id) {
        return config.processors().containsKey(id);
    }

    /// Whether an exporter is registered under `id`.
    ///
    /// @param id the component id
    /// @return true if an exporter is registered
    public boolean hasExporter(ComponentId id) {
        return config.exporters().containsKey(id);
    }

    /// Whether a connector is registered under `id`.
    ///
    /// @param id the component id
    /// @return true if a connector is registered
    public boolean hasConnector(ComponentId id) {
        return config.connectors().containsKey(id);
    }

    /// Whether an extension is registered under `id`.
    ///
    /// @param id the component id
    /// @return true if an extension is registered
    public boolean hasExtension(ComponentId id) {
        return config.extensions().containsKey(id);
    }

    /// The receiver entry registered under `id`, or null when none is.
    ///
    /// @param id the component id
    /// @return the receiver entry, or null
    public @Nullable ReceiverEntry<?> receiver(ComponentId id) {
        return config.receivers().get(id);
    }

    /// The processor entry registered under `id`, or null when none is.
    ///
    /// @param id the component id
    /// @return the processor entry, or null
    public @Nullable ProcessorEntry<?> processor(ComponentId id) {
        return config.processors().get(id);
    }

    /// The exporter entry registered under `id`, or null when none is.
    ///
    /// @param id the component id
    /// @return the exporter entry, or null
    public @Nullable ExporterEntry<?> exporter(ComponentId id) {
        return config.exporters().get(id);
    }

    /// The connector entry registered under `id`, or null when none is.
    ///
    /// @param id the component id
    /// @return the connector entry, or null
    public @Nullable ConnectorEntry<?> connector(ComponentId id) {
        return config.connectors().get(id);
    }

    /// The extension entry registered under `id`, or null when none is.
    ///
    /// @param id the component id
    /// @return the extension entry, or null
    public @Nullable ExtensionEntry<?> extension(ComponentId id) {
        return config.extensions().get(id);
    }

    /// Whether the registered receiver under `id` supports `signal`. A receiver whose factory does not
    /// declare [SignalSupport] is treated as supporting every signal.
    ///
    /// @param id     the receiver id
    /// @param signal the signal to check
    /// @return true if supported (or the factory declares no restriction)
    public boolean receiverSupports(ComponentId id, SignalDescriptor<?> signal) {
        return supports(config.receivers(), id, signal);
    }

    /// Whether the registered processor under `id` supports `signal`. A processor whose factory does
    /// not declare [SignalSupport] is treated as supporting every signal.
    ///
    /// @param id     the processor id
    /// @param signal the signal to check
    /// @return true if supported (or the factory declares no restriction)
    public boolean processorSupports(ComponentId id, SignalDescriptor<?> signal) {
        return supports(config.processors(), id, signal);
    }

    /// Whether the registered exporter under `id` supports `signal`. An exporter whose factory does
    /// not declare [SignalSupport] is treated as supporting every signal.
    ///
    /// @param id     the exporter id
    /// @param signal the signal to check
    /// @return true if supported (or the factory declares no restriction)
    public boolean exporterSupports(ComponentId id, SignalDescriptor<?> signal) {
        return supports(config.exporters(), id, signal);
    }

    private static boolean supports(
            Map<ComponentId, ?> map, ComponentId id, SignalDescriptor<?> signal) {
        var entry = map.get(id);
        Object factory = factoryOf(entry);
        if (factory instanceof SignalSupport support) {
            return support.supportsSignal(signal);
        }
        return true;
    }

    private static @Nullable Object factoryOf(@Nullable Object entry) {
        return switch (entry) {
            case ReceiverEntry<?> r -> r.factory();
            case ProcessorEntry<?> p -> p.factory();
            case ExporterEntry<?> e -> e.factory();
            case null, default -> null;
        };
    }
}
