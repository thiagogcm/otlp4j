package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// A factory for exporters. It creates one exporter bound to a signal, the terminal consumer of a
/// pipeline. The signal is supplied at creation so the same factory can produce an exporter for any
/// signal it supports; the multi-signal facet exporter over one shared client is the wrapped
/// transport client presented through per-signal exporters.
///
/// @param <C> the typed configuration this factory accepts
public interface ExporterFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one exporter for `signal`.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param config the exporter configuration
    /// @param signal the signal this exporter operates on
    /// @param <T>    the payload type
    /// @return the exporter
    <T extends TelemetryPayload> Exporter<T> create(CreateContext ctx, C config, SignalDescriptor<T> signal);
}
