package dev.nthings.otlp4j.pipeline;

/// The structural classification of a pipeline's terminal, computed at build time (when the
/// terminal reference is in hand) and reported by [PipelineHandle#describe] and consulted by
/// [Wiring#validate]. A terminal hidden behind a bare lambda classifies as [#OPAQUE_LAMBDA]: its
/// captures are invisible to reflection-free code.
enum TerminalShape {

    /// A directly attached `Batcher`.
    BATCHER,

    /// A signal-changing `Connector`.
    CONNECTOR,

    /// A `Processor` bound to a downstream via `Processor.asSink`.
    PROCESSOR_SINK,

    /// A multi-peer `FanOut`.
    FANOUT,

    /// A [SharedLifecycle] terminal that is none of the typed shapes (e.g. an exporter facet).
    SHARED_LIFECYCLE,

    /// An [AutoCloseable] terminal that is none of the typed shapes.
    AUTO_CLOSEABLE,

    /// A bare lambda (or other opaque) terminal with no visible closeable: its captures cannot be
    /// introspected, so a closeable it hides must be declared with `Stage.owns(...)`.
    OPAQUE_LAMBDA
}
