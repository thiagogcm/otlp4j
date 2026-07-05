package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/// A first-class, extensible, same-signal processing stage: it consumes a batch and forwards zero,
/// one, or many batches of the *same* signal to the supplied `next` sink, returning the ack that
/// [Pipeline.Stage#via(Processor)] delivers upstream.
///
/// This is the SAM users implement. A stateless processor is a lambda; a stateful one (buffering,
/// enriching from a resource) additionally implements [Lifecycle] and is drained automatically -
/// exactly as an [AsyncTransform] that implements [AutoCloseable] is. For a named stateless
/// processor use [dev.nthings.otlp4j.pipeline.ops.Processors]; for a stateful base with a
/// captured-`next` accessor and lifecycle hooks extend
/// [dev.nthings.otlp4j.pipeline.ops.AbstractProcessor].
///
/// The four signals expose their own SAMs - [TracesProcessor], [MetricsProcessor], [LogsProcessor],
/// [ProfilesProcessor] - which is what user code plugs lambdas into.
///
/// Unlike [Transform]/[AsyncTransform] (strict 1-to-1, drop via [Pipeline.Stage#filter]), a processor
/// controls delivery itself: it may drop a batch (return [ConsumeResult#acceptedStage()] without
/// calling `next`), coalesce many inputs into one `next.consume` (see
/// [dev.nthings.otlp4j.pipeline.ops.Batcher]), or split one input into several. It stays same-signal
/// (`T` in, `T` out); a signal *change* is [dev.nthings.otlp4j.pipeline.ops.Connector].
///
/// @param <T> the OTLP signal processed and forwarded
@FunctionalInterface
public interface Processor<T> {

    /// Processes one batch and forwards to `next` as it sees fit; returns the ack for this input.
    /// A throw, a null stage, or a failed stage is normalized to a permanent rejection by the
    /// attachment boundary ([Pipeline.Stage#via(Processor)] or [#asSink(Sink)]); the implementation
    /// never has to defend that boundary.
    ///
    /// @param batch the input batch
    /// @param next  the rest of the chain (or the bound downstream) for this signal
    /// @return a stage completing with the ack for `batch`
    CompletionStage<ConsumeResult> process(T batch, Sink<? super T> next);

    /// Binds `downstream` as a fixed `next`, yielding a terminal [Sink] usable outside the pipeline
    /// DSL (or as a fan-out peer). The returned sink cascades lifecycle the way
    /// [dev.nthings.otlp4j.pipeline.ops.Connector] does: draining it drains this processor (if a
    /// [Lifecycle]) then the downstream (if a [Lifecycle]), and a [SharedLifecycle] on *either* side
    /// is reference-counted. Prefer [Pipeline.Stage#via(Processor)] for mid-chain use, where the
    /// downstream stays structurally visible to the pipeline (fan-out peers included); use `asSink`
    /// only when a captured, single downstream is genuinely wanted.
    ///
    /// (`Batcher` overrides this to preserve its documented non-cascading contract - see
    /// [dev.nthings.otlp4j.pipeline.ops.Batcher#asSink(Sink)].)
    ///
    /// @param downstream the fixed next sink
    /// @return a terminal sink that forwards through this processor
    default Sink<T> asSink(Sink<? super T> downstream) {
        Objects.requireNonNull(downstream, "downstream");
        return new BoundProcessorSink<>(this, downstream);
    }
}
