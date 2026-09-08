/// The built-in routing and filtering processors: a shared match engine over resource-attribute and
/// client-metadata-header conditions, with an escape hatch onto the materialised domain batch for a
/// match that cannot be expressed against those two cheap axes. The filter processor is a stand-alone
/// keep-or-drop stage in one pipeline; the routing processor is one branch of a content-routing
/// fan-out, forwarding to its single downstream only when its own configured condition matches. Both
/// read `Payload.routingMetadata()` and `ConsumeContext.metadata()` without materialising the
/// domain, unless a rule's domain-predicate escape hatch is used. [MatchSpec#all()] and
/// [MatchSpec#none()] make catch-all and no-match branches explicit.
package dev.nthings.otlp4j.components.routing;
