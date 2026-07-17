package dev.nthings.otlp4j.core.component;

/// The marker for a component's typed configuration record. Each component factory declares the
/// concrete config type it accepts as its type parameter, so a config is checked against its factory
/// at compile time rather than being an untyped property map.
///
/// A factory whose component carries no settings declares [None] as its config type and returns the
/// shared [#NONE] singleton from `defaultConfig()`, replacing the private empty-config copies that
/// otherwise accrete in every module that hosts such a factory.
public interface ComponentConfig {

    /// The shared no-op configuration for a component that carries no settings.
    None NONE = new None();

    /// The empty configuration record: a component that is fully described by its factory carries no
    /// configurable state, so this record has no fields and every such factory shares the single
    /// [#NONE] instance rather than copying its own empty record.
    record None() implements ComponentConfig {}
}
