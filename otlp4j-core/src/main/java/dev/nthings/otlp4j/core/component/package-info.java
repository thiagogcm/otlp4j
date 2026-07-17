/// Component identity, kind, the component lifecycle contract, and the factory contracts the gateway
/// builds every component through. `ComponentId` is the graph node identity in the Collector
/// `type[/name]` form; `ComponentKind` classifies a component. `Component` and its start, flush, and
/// shutdown contexts are the deadline-bounded lifecycle; the component interfaces and their factories
/// are the extension surface a third-party component author implements.
@NullMarked
package dev.nthings.otlp4j.core.component;

import org.jspecify.annotations.NullMarked;
