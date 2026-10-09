/// The built-in filter processor and its match engine over resource-attribute and
/// client-metadata-header conditions, with an escape hatch onto the materialised domain batch for a
/// match that cannot be expressed against those two cheap axes. The filter decides per resource group:
/// it forwards the groups its configured condition keeps and drops the rest, so a payload that mixes
/// resources is never forwarded or dropped on the strength of one of them. Pipelines that share a
/// source and each start with a filter route a stream to several destinations. Matching reads
/// `Payload.routingMetadata()` and `ConsumeContext.metadata()` without materialising the domain,
/// unless a rule's domain-predicate escape hatch is used. [MatchSpec#all()] and [MatchSpec#none()]
/// make catch-all and no-match conditions explicit.
package dev.nthings.otlp4j.components.routing;
