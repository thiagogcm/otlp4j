package dev.nthings.otlp4j.pipeline;

import java.util.Objects;

/// One issue found by [Wiring#validate]: a [Kind] and a human-readable detail. Cheap structural
/// findings over the declared topology, honest about the limit that reflection-free code cannot see
/// resources captured inside a lambda terminal (see [Kind#OPAQUE_TERMINAL]).
///
/// @param kind   the kind of issue
/// @param detail a human-readable description
public record ValidationIssue(Kind kind, String detail) {

    /// Canonical constructor.
    public ValidationIssue {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(detail, "detail");
    }

    /// The kinds of issue [Wiring#validate] can detect.
    public enum Kind {

        /// Pipelines were declared but no receiver ingests into them. Exact.
        NO_RECEIVERS,

        /// Receivers were declared but no pipeline drains them. Exact.
        NO_PIPELINES,

        /// A pipeline's terminal is a bare lambda (or a fan-out of only lambdas) and no
        /// `Stage.owns(...)` was declared: a partial, advisory heuristic, since a closeable the
        /// lambda captures is invisible and would leak.
        OPAQUE_TERMINAL,

        /// A resource registered with `Wiring.Builder.owns(...)` is also auto-collected by a
        /// pipeline. Harmless (dedup covers it), reported for hygiene. Exact, by identity.
        REDUNDANT_OWNS,

        /// The same non-[SharedLifecycle] [Lifecycle] instance is attached to two pipelines, so it
        /// would be shut down twice with no reference count to protect it. Exact, by identity.
        DOUBLE_ATTACHED_UNSHARED
    }
}
