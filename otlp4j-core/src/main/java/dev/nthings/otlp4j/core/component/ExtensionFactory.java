package dev.nthings.otlp4j.core.component;

/// A factory for extensions. It creates one extension instance from its configuration. This is the
/// contract only in the first milestone; the first concrete extensions land in a later phase, but
/// the kind and lifecycle slot exist now.
///
/// @param <C> the typed configuration this factory accepts
public interface ExtensionFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one extension.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param config the extension configuration
    /// @return the extension
    Extension create(CreateContext ctx, C config);
}
