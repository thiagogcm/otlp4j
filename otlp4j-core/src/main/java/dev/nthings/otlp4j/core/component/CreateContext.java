package dev.nthings.otlp4j.core.component;

/// The context a factory receives to create one component instance: the component's identity, the
/// base component context (the metrics handle and clock) the created component starts with, and the
/// build-scoped [SharedComponents] cache through which a factory shares one underlying instance across
/// the several per-signal facets it produces for a single id.
///
/// @param id               the identity the created component is registered under
/// @param base             the base component context the component starts with
/// @param sharedComponents the build-scoped shared-component cache, keyed by component id
public record CreateContext(ComponentId id, ComponentContext base, SharedComponents sharedComponents) {

    /// Validates that every component is present.
    public CreateContext {
        if (id == null) {
            throw new IllegalArgumentException("component id must be non-null");
        }
        if (base == null) {
            throw new IllegalArgumentException("base component context must be non-null");
        }
        if (sharedComponents == null) {
            throw new IllegalArgumentException("shared components must be non-null");
        }
    }

    /// Builds a create context with a fresh, private [SharedComponents] cache, for a caller that
    /// creates one component in isolation rather than through a shared gateway build.
    ///
    /// @param id   the identity the created component is registered under
    /// @param base the base component context the component starts with
    public CreateContext(ComponentId id, ComponentContext base) {
        this(id, base, new SharedComponents());
    }
}
