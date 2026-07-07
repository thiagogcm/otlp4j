package dev.nthings.otlp4j.core.component;

/// The base factory contract every component factory implements: it declares the component kind it
/// produces, its stability level, its type name (the `type` half of a `ComponentId`), and a default
/// configuration instance. The kind-specific factories add a `create` method with the arguments that
/// kind needs.
///
/// @param <C> the typed configuration this factory accepts
public interface ComponentFactory<C extends ComponentConfig> {

    /// The kind of component this factory produces.
    ///
    /// @return the component kind
    ComponentKind kind();

    /// The stability level of the component this factory produces.
    ///
    /// @return the stability level
    StabilityLevel stability();

    /// A default configuration instance for the component.
    ///
    /// @return the default config
    C defaultConfig();

    /// The component type name, forming the `type` half of a `ComponentId`.
    ///
    /// @return the type name
    String type();
}
