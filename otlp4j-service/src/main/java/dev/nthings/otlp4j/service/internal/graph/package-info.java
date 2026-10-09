/// The internal service graph: the directed acyclic graph of pipeline nodes and typed edges. No
/// module exports this package, so no graph vertex or edge type ever crosses a module boundary. The
/// graph algorithm, the node and edge model, and the fan-out vertex all live here, behind the
/// [dev.nthings.otlp4j.service.internal.graph.ServiceGraph] surface the gateway builder drives.
@NullMarked
package dev.nthings.otlp4j.service.internal.graph;

import org.jspecify.annotations.NullMarked;
