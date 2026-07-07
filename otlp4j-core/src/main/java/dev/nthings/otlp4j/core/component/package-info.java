/// Component identity, kind, stability, the component lifecycle contract, and the factory contracts
/// the gateway builds every component through. `ComponentId` is the graph node identity in the
/// Collector `type[/name]` form; `ComponentKind` and `StabilityLevel` classify a component and
/// declare how far along the 0.x stability ladder it ships. `Component` and its start, flush, and
/// shutdown contexts are the deadline-bounded lifecycle; the five component interfaces and their
/// factories are the extension surface a third-party component author implements.
@NullMarked
package dev.nthings.otlp4j.core.component;

import org.jspecify.annotations.NullMarked;
