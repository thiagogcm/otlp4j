package dev.nthings.otlp4j.core.signal;

import dev.nthings.otlp4j.model.Attributes;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Core implementation shared by domain-backed payloads and codec adapters. */
final class RoutingMetadataSupport implements RoutingMetadata {

    private final List<Attributes> resources;

    private RoutingMetadataSupport(List<Attributes> resources) {
        this.resources = List.copyOf(resources);
    }

    static RoutingMetadata of(List<Attributes> resources) {
        if (resources == null) {
            throw new IllegalArgumentException("resources must be non-null");
        }
        return new RoutingMetadataSupport(resources);
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
