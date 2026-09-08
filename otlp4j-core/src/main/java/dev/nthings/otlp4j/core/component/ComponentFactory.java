package dev.nthings.otlp4j.core.component;

/// The base factory contract every component factory implements: it declares the component kind it
/// produces, its type name (the `type` half of a `ComponentId`), and a default configuration
/// instance. The kind-specific factories add a `create` method with the arguments that kind needs.
/// A factory that allocates resources while creating a component owns those allocations until the
/// component is successfully returned; if creation fails, it must release them before propagating
/// the failure. Callers can clean up only a component instance that was returned successfully.
///
/// @param <C> the typed configuration this factory accepts
public interface ComponentFactory<C extends ComponentConfig> {

    /// The kind of component this factory produces.
    ///
    /// @return the component kind
    ComponentKind kind();

    /// A default configuration instance for the component.
    ///
    /// @return the default config
    C defaultConfig();

    /// The component type name, forming the `type` half of a `ComponentId`.
    ///
    /// @return the type name
    String type();
}
