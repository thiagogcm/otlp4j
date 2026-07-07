package dev.nthings.otlp4j.core.component;

/// The context a factory receives to create one component instance: the component's identity, the
/// base component context (the metrics handle and clock) the created component starts with, the
/// build-scoped [SharedComponents] cache through which a factory shares one underlying instance across
/// the several per-signal facets it produces for a single id, and the [Extensions] view through which
/// a data-path factory resolves an extension (an authenticator or credential) an id references.
///
/// @param id               the identity the created component is registered under
/// @param base             the base component context the component starts with
/// @param sharedComponents the build-scoped shared-component cache, keyed by component id
/// @param extensions       the build-scoped extension resolver, empty when none are built
public record CreateContext(ComponentId id, ComponentContext base, SharedComponents sharedComponents,
                            Extensions extensions) {

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
        if (extensions == null) {
            throw new IllegalArgumentException("extensions must be non-null; use Extensions.empty()");
        }
    }

    /// Builds a create context with an empty [Extensions] view, for a caller that shares a build's
    /// component cache but resolves no extension.
    ///
    /// @param id               the identity the created component is registered under
    /// @param base             the base component context the component starts with
    /// @param sharedComponents the build-scoped shared-component cache
    public CreateContext(ComponentId id, ComponentContext base, SharedComponents sharedComponents) {
        this(id, base, sharedComponents, Extensions.empty());
    }

    /// Builds a create context with a fresh, private [SharedComponents] cache and an empty
    /// [Extensions] view, for a caller that creates one component in isolation rather than through a
    /// shared gateway build.
    ///
    /// @param id   the identity the created component is registered under
    /// @param base the base component context the component starts with
    public CreateContext(ComponentId id, ComponentContext base) {
        this(id, base, new SharedComponents(), Extensions.empty());
    }
}
