package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;

/// A factory for processors. It creates one processor bound to a signal, forwarding to the next
/// consumer in the pipeline. The signal and the next consumer are supplied at creation so the same
/// factory can produce a processor for any signal it supports.
///
/// @param <C> the typed configuration this factory accepts
public interface ProcessorFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one processor for `signal` that forwards to `next`.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param config the processor configuration
    /// @param signal the signal this processor operates on
    /// @param next   the downstream consumer this processor forwards to
    /// @param <T>    the payload type
    /// @return the processor
    <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, C config, SignalDescriptor<T> signal, SignalConsumer<T> next);

    /// Builds a synchronous domain-transform processor of type `type` operating on `signal`. Its
    /// component reads the incoming payload through the interface-level
    /// [PayloadView#DOMAIN_IMMUTABLE] view (materialising the domain once, on demand, whether the
    /// upstream handed it a proto-backed or a domain-backed payload), applies `transform`, and either
    /// drops the batch (a `null` transform result, reported as accepted with nothing forwarded) or
    /// wraps the transformed batch through `payloads` and forwards it downstream. Because the domain
    /// records are immutable the processor is non-mutating, so it needs no fan-out copy.
    ///
    /// This collapses the per-component factory-plus-processor boilerplate a trivial transform
    /// otherwise costs to a single call. `payloads` is the emit seam (`Payloads.DOMAIN` from the
    /// codec) that keeps this factory in `otlp4j-core` without a codec dependency; see [PayloadFactory].
    ///
    /// @param type      the component type name
    /// @param signal    the signal this processor operates on
    /// @param transform the batch transform, returning `null` to drop the batch
    /// @param payloads  the seam that wraps a transformed batch as a payload
    /// @param <M>       the domain batch type this processor transforms
    /// @return a processor factory over the no-op configuration
    static <M> ProcessorFactory<ComponentConfig.None> of(
            String type,
            SignalDescriptor<M> signal,
            UnaryOperator<@Nullable M> transform,
            PayloadFactory payloads) {
        return new FunctionProcessorFactory<>(type, signal, transform, payloads);
    }

    /// Reinterprets the payload-typed downstream consumer the graph supplies as one keyed by the
    /// opaque [TelemetryPayload], the single sanctioned unchecked cast a processor factory needs to
    /// wire its component to the graph. At runtime the consumer is agnostic to the concrete payload
    /// backing, so the reinterpretation is safe.
    ///
    /// @param next the downstream consumer
    /// @return the same consumer typed over [TelemetryPayload]
    @SuppressWarnings("unchecked")
    static SignalConsumer<TelemetryPayload> asPayloadConsumer(SignalConsumer<?> next) {
        return (SignalConsumer<TelemetryPayload>) next;
    }

    /// Reinterprets a payload-typed processor as the signal-typed processor the graph builder wires,
    /// the counterpart of [#asPayloadConsumer] on the produced component. At runtime the processor is
    /// agnostic to the concrete payload backing, so the reinterpretation is safe.
    ///
    /// @param processor the payload-typed processor
    /// @param <T>       the payload type the graph expects
    /// @return the same processor typed over `T`
    @SuppressWarnings("unchecked")
    static <T extends TelemetryPayload> Processor<T> asTypedProcessor(Processor<TelemetryPayload> processor) {
        return (Processor<T>) (Processor<?>) processor;
    }

    /// Reinterprets the payload-typed descriptor the graph supplies as the domain-typed descriptor a
    /// processor operates on. At runtime the descriptor is the signal's domain-batch descriptor, so
    /// this reinterpretation is safe.
    ///
    /// @param signal the payload-typed descriptor
    /// @param <M>    the domain batch type
    /// @return the same descriptor typed over `M`
    @SuppressWarnings("unchecked")
    static <M> SignalDescriptor<M> asDomainDescriptor(SignalDescriptor<?> signal) {
        return (SignalDescriptor<M>) signal;
    }

    /// The [#of] factory: it captures the signal it operates on, the batch transform, and the emit
    /// seam, and validates at creation that the graph asks it for the signal it was built for.
    ///
    /// @param <M> the model batch type this processor transforms
    final class FunctionProcessorFactory<M> implements ProcessorFactory<ComponentConfig.None> {

        private final String type;
        private final SignalDescriptor<M> signal;
        private final UnaryOperator<@Nullable M> transform;
        private final PayloadFactory payloads;

        private FunctionProcessorFactory(
                String type,
                SignalDescriptor<M> signal,
                UnaryOperator<@Nullable M> transform,
                PayloadFactory payloads) {
            if (type == null || signal == null || transform == null || payloads == null) {
                throw new IllegalArgumentException("type, signal, transform, and payloads must be non-null");
            }
            this.type = type;
            this.signal = signal;
            this.transform = transform;
            this.payloads = payloads;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.PROCESSOR;
        }

        @Override
        public ComponentConfig.None defaultConfig() {
            return ComponentConfig.NONE;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public <T extends TelemetryPayload> Processor<T> create(
                CreateContext ctx, ComponentConfig.None config, SignalDescriptor<T> reqSignal, SignalConsumer<T> next) {
            if (!reqSignal.equals(signal)) {
                throw new IllegalArgumentException(
                        "processor " + type + " operates on " + signal + " but was created for " + reqSignal);
            }
            Processor<TelemetryPayload> processor =
                    new FunctionProcessor<>(ctx.id(), signal, transform, payloads, asPayloadConsumer(next));
            return asTypedProcessor(processor);
        }
    }

    /// The [#of] processor: it unwraps the model batch through the domain view, applies the transform,
    /// and either drops the batch or wraps the transformed batch and forwards it downstream.
    ///
    /// @param <M> the model batch type this processor transforms
    final class FunctionProcessor<M> implements Processor<TelemetryPayload> {

        private final SignalDescriptor<M> signal;
        private final UnaryOperator<@Nullable M> transform;
        private final PayloadFactory payloads;
        private final SignalConsumer<TelemetryPayload> next;
        private final Provenance provenance;

        private FunctionProcessor(
                ComponentId id,
                SignalDescriptor<M> signal,
                UnaryOperator<@Nullable M> transform,
                PayloadFactory payloads,
                SignalConsumer<TelemetryPayload> next) {
            this.signal = signal;
            this.transform = transform;
            this.payloads = payloads;
            this.next = next;
            this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
            var in = signal.batchType().cast(data.view(PayloadView.DOMAIN_IMMUTABLE));
            var transformed = transform.apply(in);
            if (transformed == null) {
                return DeliveryResult.acceptedStage(provenance);
            }
            return next.consume(context, payloads.domain(signal, transformed));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
