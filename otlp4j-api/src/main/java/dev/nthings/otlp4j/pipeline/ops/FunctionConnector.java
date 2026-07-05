package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.pipeline.Sink;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// A [Connector] whose [#derive] is an arbitrary [Function], keyed by its input and output
/// [Signal]s. Backs the general [Connectors#of] factory so adding a conversion is one call, not a
/// new subclass. The `(in, out)` pair makes the connector self-describe its conversion in
/// [#toString].
///
/// @param <I> the input signal consumed by this connector
/// @param <O> the derived signal emitted downstream
final class FunctionConnector<I, O> extends Connector<I, O> {

    private final Signal<I> in;
    private final Signal<O> out;
    private final Function<I, @Nullable O> derive;

    FunctionConnector(
            String name,
            Signal<I> in,
            Signal<O> out,
            List<? extends Sink<? super O>> downstreams,
            FailurePolicy policy,
            Function<I, @Nullable O> derive) {
        super(name, downstreams, policy);
        this.in = Objects.requireNonNull(in, "in");
        this.out = Objects.requireNonNull(out, "out");
        this.derive = Objects.requireNonNull(derive, "derive");
    }

    @Override
    protected @Nullable O derive(I batch) {
        return derive.apply(batch);
    }

    @Override
    public String toString() {
        return name() + " [" + in.signalName() + "->" + out.signalName() + "]";
    }
}
