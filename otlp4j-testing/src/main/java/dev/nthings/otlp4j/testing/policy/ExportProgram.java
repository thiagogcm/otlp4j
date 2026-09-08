package dev.nthings.otlp4j.testing.policy;

import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import java.util.List;
import java.util.Objects;

/// One programmed export: the neutral policies that configure the exporter under test and the script
/// the programmed server replays. The script's length and contents drive the server one attempt at a
/// time; the policies drive the exporter's retry and timeout behaviour. A single program runs unchanged
/// on both wire protocols through the seam.
///
/// @param retry   the retry policy the exporter is configured with
/// @param timeout the timeout policy the exporter is configured with
/// @param script  the per-attempt server behaviours, replayed in order
public record ExportProgram(RetryPolicy retry, TimeoutPolicy timeout, List<ProgrammedResponse> script) {

    /// Validates the policies and copies the script defensively.
    public ExportProgram {
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(timeout, "timeout");
        script = List.copyOf(script);
    }
}
