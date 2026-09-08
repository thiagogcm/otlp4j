package dev.nthings.otlp4j.core.signal;

import java.util.Optional;
import java.util.Set;

/// The cheap, proto-free resource metadata a [Payload] exposes for routing and filtering. The header
/// axis is carried separately by the consume context's client metadata. Resource metadata deliberately
/// excludes item-level attributes because reading those would require walking the payload.
public interface RoutingMetadata {

    /// The string value of a resource-level attribute, if present as a string on any resource in the
    /// payload. When several resources carry the key, the first match in wire order wins. This is the
    /// common tenancy or service routing key.
    ///
    /// @param key the resource-attribute key to look up
    /// @return the attribute value, or empty when absent or not string-valued
    Optional<String> resourceAttribute(String key);

    /// The set of resource-attribute keys present across the payload's resources.
    ///
    /// @return the resource-attribute key set
    Set<String> resourceAttributeKeys();
}
