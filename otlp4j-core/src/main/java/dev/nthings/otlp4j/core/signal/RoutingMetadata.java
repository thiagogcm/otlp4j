package dev.nthings.otlp4j.core.signal;

import java.util.Optional;
import java.util.Set;

/// The cheap, proto-free routing surface a [TelemetryPayload] exposes through the
/// [PayloadView#ROUTING_METADATA] view, for the routing and filtering processors to read a routing key
/// without materialising the domain batch.
///
/// It covers only the payload-attribute axis of routing: the resource-level attributes (service name,
/// tenant id, deployment environment) that a routing or filtering rule keys on. The header axis is
/// carried separately by the consume context's client metadata, so this view deliberately does not
/// duplicate it. It exposes only resource attributes, never span, data-point, or log-record
/// attributes, because those cannot be read without walking the full payload, which is exactly what
/// the view exists to avoid.
///
/// A routing key is looked up as a plain string rather than as a model attribute value, because
/// routing keys are string-valued in practice and a string lookup keeps a routing processor off the
/// model type on the hot path. When a payload carries several resources, an implementation merges
/// their attributes so a multi-resource batch answers a routing question once.
public interface RoutingMetadata {

    /// The signal this payload carries, for signal-based routing.
    ///
    /// @return the signal descriptor
    SignalDescriptor<?> signal();

    /// The number of OTLP items in the payload, for a size guard on a routing decision.
    ///
    /// @return the item count
    long itemCount();

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
