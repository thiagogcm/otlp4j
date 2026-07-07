package dev.nthings.otlp4j.core.component;

import java.util.Optional;

/// The build-scoped view through which a data-path factory resolves an [Extension] by id at component
/// create time. It is carried on the [CreateContext], so a receiver, exporter, or connector factory
/// can look up an authenticator or credential extension a configuration references and attach it to
/// the component it builds.
///
/// The graph builds extensions before the data path and wraps the built map in an `Extensions` view,
/// so every data-path `create` call can reach an already-created extension. A caller that builds a
/// component in isolation, with no extensions, uses [#empty].
public interface Extensions {

    /// The shared empty view, resolving no extension. Used as the default on a create context built
    /// without a gateway, and reachable through [#empty].
    Extensions EMPTY = id -> Optional.empty();

    /// The extension registered under `id`, if any.
    ///
    /// @param id the extension's component id
    /// @return the extension, or empty when none is registered under the id
    Optional<Extension> get(ComponentId id);

    /// The shared empty view, resolving no extension.
    ///
    /// @return the empty extensions view
    static Extensions empty() {
        return EMPTY;
    }
}
