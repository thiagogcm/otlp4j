package dev.nthings.otlp4j.core.component;

/// The marker for a component's typed configuration record. Each component factory declares the
/// concrete config type it accepts as its type parameter, so a config is checked against its factory
/// at compile time rather than being an untyped property map.
public interface ComponentConfig {
}
