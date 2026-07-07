package dev.nthings.otlp4j.samples.support;

import dev.nthings.otlp4j.core.component.ComponentConfig;

/// The (empty) configuration for a [FunctionProcessorFactory]. The transform the processor applies is
/// a property of the factory instance, not a value carried in configuration, so this record has no
/// fields; it exists only to satisfy the typed [ComponentConfig] contract every factory declares.
public record FunctionProcessorConfig() implements ComponentConfig {

    private static final FunctionProcessorConfig DEFAULT = new FunctionProcessorConfig();

    /// The shared default configuration.
    ///
    /// @return the default config
    public static FunctionProcessorConfig defaults() {
        return DEFAULT;
    }
}
