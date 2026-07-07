package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Attributes;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/// The concrete [RoutingMetadata] both payloads return for the
/// [dev.nthings.otlp4j.core.signal.PayloadView#ROUTING_METADATA] view. It is built over the payload's
/// signal, its item count, and the per-resource attributes already extracted in wire order, so a
/// lookup merges the resources of a multi-resource batch and answers a routing question once.
///
/// It carries only the resource-attribute lists a proto or domain payload can produce without
/// materialising the domain: the domain path reads the resource wrappers directly, the proto path
/// converts only the top-level resource attribute lists. Neither path decodes the spans, data points,
/// log records, or profiles.
final class RoutingMetadataView implements RoutingMetadata {

    private final SignalDescriptor<?> signal;
    private final long itemCount;
    private final List<Attributes> resources;

    RoutingMetadataView(SignalDescriptor<?> signal, long itemCount, List<Attributes> resources) {
        this.signal = signal;
        this.itemCount = itemCount;
        this.resources = resources;
    }

    @Override
    public SignalDescriptor<?> signal() {
        return signal;
    }

    @Override
    public long itemCount() {
        return itemCount;
    }

    @Override
    public Optional<String> resourceAttribute(String key) {
        for (var attributes : resources) {
            var value = attributes.getString(key);
            if (value != null) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    @Override
    public Set<String> resourceAttributeKeys() {
        var keys = new LinkedHashSet<String>();
        for (var attributes : resources) {
            keys.addAll(attributes.keys());
        }
        return Collections.unmodifiableSet(keys);
    }
}
