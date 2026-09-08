package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.model.Attributes;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/// Immutable routing metadata backed by resource attributes extracted from a domain batch or only
/// from the top-level wrappers of a protobuf request.
final class RoutingMetadataView implements RoutingMetadata {

    private final List<Attributes> resources;

    RoutingMetadataView(List<Attributes> resources) {
        this.resources = List.copyOf(resources);
    }

    @Override
    public Optional<String> resourceAttribute(String key) {
        if (key == null) {
            return Optional.empty();
        }
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
