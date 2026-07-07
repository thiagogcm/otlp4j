package dev.nthings.otlp4j.core.pipeline;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Optional;

/// A pipeline's identity: a signal and an optional name, in the Collector `signal[/name]` string
/// form. `traces` names the default traces pipeline; `traces/high-priority` names a second,
/// distinctly configured traces pipeline; `logs/audit` a named logs pipeline. Used as a `Map` key in
/// the gateway config, so it is a plain value record.
///
/// @param signal the signal this pipeline carries
/// @param name   the optional pipeline name
public record PipelineId(SignalDescriptor<?> signal, Optional<String> name) {

    /// Validates the components: the signal is required, and a present name must be non-blank.
    public PipelineId {
        if (signal == null) {
            throw new IllegalArgumentException("pipeline signal must be non-null");
        }
        if (name == null) {
            throw new IllegalArgumentException("pipeline name must be non-null; use Optional.empty()");
        }
        if (name.isPresent() && name.get().isBlank()) {
            throw new IllegalArgumentException("pipeline name, when present, must be non-blank");
        }
    }

    /// The default (unnamed) pipeline for `signal`.
    ///
    /// @param signal the signal
    /// @return the pipeline id
    public static PipelineId of(SignalDescriptor<?> signal) {
        return new PipelineId(signal, Optional.empty());
    }

    /// The named pipeline `signal/name`.
    ///
    /// @param signal the signal
    /// @param name   the pipeline name
    /// @return the pipeline id
    public static PipelineId of(SignalDescriptor<?> signal, String name) {
        return new PipelineId(signal, Optional.of(name));
    }

    /// Renders the `signal[/name]` string form.
    @Override
    public String toString() {
        return name.map(n -> signal.signalName() + "/" + n).orElseGet(signal::signalName);
    }
}
