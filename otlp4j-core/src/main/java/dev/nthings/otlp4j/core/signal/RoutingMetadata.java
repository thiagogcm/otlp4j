package dev.nthings.otlp4j.core.signal;

import dev.nthings.otlp4j.model.Attributes;
import java.util.List;

/// The cheap, proto-free resource metadata a [Payload] exposes for routing and filtering. The header
/// axis is carried separately by the consume context's client metadata. Resource metadata deliberately
/// excludes item-level attributes because reading those would require walking the payload.
public interface RoutingMetadata {

    /// Routing metadata over the given resource attribute sets.
    ///
    /// @param resources the attributes of each resource group, in wire order
    /// @return immutable routing metadata
    static RoutingMetadata of(List<Attributes> resources) {
        if (resources == null) {
            throw new IllegalArgumentException("resources must be non-null");
        }
        return new ResourceRoutingMetadata(resources);
    }

    /// The attributes of each resource group in the payload, in wire order. A payload that mixes
    /// resources has one entry per group, so a routing decision can be made for each group rather
    /// than for the payload as a whole.
    ///
    /// @return the immutable per-resource attributes
    List<Attributes> resources();
}
