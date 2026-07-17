/// Internal runtime helpers the gateway builder wires but no module exports: the guarded consumer
/// that normalizes every delivery through [GraphNodeBoundary], the fan-out
/// consumer that merges peer results, and the routed-consumers bundle a receiver factory receives.
@NullMarked
package dev.nthings.otlp4j.service.internal;

import org.jspecify.annotations.NullMarked;
