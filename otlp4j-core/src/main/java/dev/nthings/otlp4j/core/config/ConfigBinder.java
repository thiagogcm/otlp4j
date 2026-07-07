package dev.nthings.otlp4j.core.config;

import dev.nthings.otlp4j.core.component.ComponentConfig;

/// An optional capability a component factory implements to bind its own configuration sub-map into
/// its typed config record. A declarative loader calls [#bind] with the component's [ConfigNode] and
/// the factory's default config, and the factory reads the keys it understands off the node and folds
/// them onto the defaults.
///
/// Binding is type-safe and reflection-free: the factory reads named, typed values through the node
/// rather than the loader populating a record by reflection, so no module needs to open a package for
/// a validating record constructor. A factory that does not implement this capability is bindable only
/// with no configuration keys; any key a document provides for it is reported unknown, because nothing
/// read it. This mirrors the optional-capability pattern of
/// [dev.nthings.otlp4j.core.component.SignalSupport].
///
/// @param <C> the typed configuration this factory binds
public interface ConfigBinder<C extends ComponentConfig> {

    /// Binds the configuration under `node` onto `defaults`, returning the resolved config. An
    /// implementation reads only the keys it understands, so the node's [ConfigNode#unreadKeys] then
    /// names every key the document supplied that this factory did not consume.
    ///
    /// @param node     the component's configuration sub-map
    /// @param defaults the factory's default configuration, the base every unread key falls back to
    /// @return the resolved configuration
    C bind(ConfigNode node, C defaults);
}
