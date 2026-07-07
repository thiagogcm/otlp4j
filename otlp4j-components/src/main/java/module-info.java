/// The built-in gateway components and their reusable helpers, depending only on the core contracts
/// and the codec payload facade. No transport, wire, or graph library appears on this module's
/// surface, so a component here can be assembled into any graph without dragging a server or client
/// runtime with it.
///
/// The `dev.nthings.otlp4j.core` contracts are re-exported transitively because a built-in factory's
/// public config record and its `SignalSupport` and `ConfigBinder` capabilities name core types. The
/// `dev.nthings.otlp4j.components.internal` package, holding the bounded delivery queue and the shared
/// match engine, is deliberately not exported: it is module-internal machinery a component reuses, not
/// a public surface.
module dev.nthings.otlp4j.components {
    requires transitive dev.nthings.otlp4j.core;
    requires dev.nthings.otlp4j.codec;
    requires org.slf4j;
    requires static transitive org.jspecify;

    exports dev.nthings.otlp4j.components.batch;
    exports dev.nthings.otlp4j.components.memory;
    exports dev.nthings.otlp4j.components.routing;
}
