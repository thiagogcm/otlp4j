package dev.nthings.otlp4j.core.signal;

import dev.nthings.otlp4j.model.Attributes;
import java.util.List;

/// Package-private immutable implementation behind [RoutingMetadata#of].
record ResourceRoutingMetadata(List<Attributes> resources) implements RoutingMetadata {

    ResourceRoutingMetadata {
        resources = List.copyOf(resources);
    }
}
