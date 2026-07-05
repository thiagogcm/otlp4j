package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.Sink;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// Factories for stateless [Processor]s: reusable, named, same-signal stages with no lifecycle. A
/// stateless processor holds no per-attachment state, so one instance may be dropped into many
/// pipelines freely (there is nothing to drain or reference-count). Stateful, resource-holding
/// stages extend [AbstractProcessor] instead and are, by default, single-attachment (see the module
/// design notes).
public final class Processors {

    private Processors() {}

    /// A processor that prunes each batch and forwards the result to `next`; a `null` mapping **drops**
    /// the batch (accepted, not forwarded). This is the reusable, named form of the common
    /// "prune-then-forward, sometimes drop" gateway stage.
    ///
    /// Note the deliberate divergence from [dev.nthings.otlp4j.pipeline.Transform], whose null return
    /// is a *contract violation* rejected permanently: here `null` is a **first-class drop** (the
    /// cardinality control that makes a processor more than a transform). The name `pruning` - not
    /// `mapping` - telegraphs that reversal so a `Transform`-style map function is not ported in
    /// expecting null to fail.
    ///
    /// @param name the processor name, for diagnostics
    /// @param fn   maps an input batch to the batch to forward, or null to drop
    /// @param <T>  the signal type
    /// @return a stateless processor
    public static <T> Processor<T> pruning(String name, Function<T, @Nullable T> fn) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(fn, "fn");
        return new NamedProcessor<>(name, (batch, next) -> {
            @Nullable T pruned = fn.apply(batch);
            return pruned == null ? ConsumeResult.acceptedStage() : next.consume(pruned);
        });
    }

    /// Wraps an arbitrary process function as a named, stateless processor (full control of `next`:
    /// drop, coalesce, or split).
    ///
    /// @param name  the processor name, for diagnostics
    /// @param logic the process function
    /// @param <T>   the signal type
    /// @return a stateless processor
    public static <T> Processor<T> of(String name, Processor<T> logic) {
        Objects.requireNonNull(name, "name");
        return new NamedProcessor<>(name, Objects.requireNonNull(logic, "logic"));
    }

    /// A named stateless processor. The name surfaces in the pipeline's failure-normalization message
    /// (see [dev.nthings.otlp4j.pipeline.Pipeline]) via `toString`.
    private record NamedProcessor<T>(String name, Processor<T> delegate) implements Processor<T> {
        @Override public CompletionStage<ConsumeResult> process(T batch, Sink<? super T> next) {
            return delegate.process(batch, next);
        }
        @Override public String toString() { return name; }
    }
}
