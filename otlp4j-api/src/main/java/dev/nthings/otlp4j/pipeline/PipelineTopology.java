package dev.nthings.otlp4j.pipeline;

import java.util.List;
import java.util.Objects;

/// The reflection-free, build-time view of a pipeline's structural topology: the terminal's
/// [TerminalShape] and runtime class, whether the builder declared any `Stage.owns(...)` resource,
/// and the [AutoCloseable] resources the subscription drains. Consumed by [Wiring#validate] and
/// [Wiring#describe]. A custom [PipelineHandle] that cannot introspect itself returns
/// [#opaque()].
///
/// @param terminal      the terminal's structural shape
/// @param terminalClass the terminal's runtime class name
/// @param ownsDeclared  whether any resource was registered via `Stage.owns(...)`
/// @param resources     the deduplicated closeable resources the pipeline drains (never the source
///                      subscription itself), for identity-based cross-pipeline checks
public record PipelineTopology(
        TerminalShape terminal, String terminalClass, boolean ownsDeclared, List<AutoCloseable> resources) {

    private static final PipelineTopology OPAQUE =
            new PipelineTopology(TerminalShape.OPAQUE_LAMBDA, "?", false, List.of());

    /// Canonical constructor, defensively copying the resource list.
    public PipelineTopology {
        Objects.requireNonNull(terminal, "terminal");
        Objects.requireNonNull(terminalClass, "terminalClass");
        resources = List.copyOf(resources);
    }

    /// The safe default for a [PipelineHandle] that cannot describe itself: an opaque terminal with
    /// no visible resources.
    ///
    /// @return the opaque topology
    public static PipelineTopology opaque() {
        return OPAQUE;
    }
}
