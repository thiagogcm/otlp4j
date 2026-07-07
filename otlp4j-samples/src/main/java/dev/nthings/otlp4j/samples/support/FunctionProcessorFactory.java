package dev.nthings.otlp4j.samples.support;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;

/// A demo seed [ProcessorFactory] whose component applies a pure batch transform and forwards the
/// result to the next consumer. This reproduces the enrich, redact, filter, and drop-empty steps the
/// end-to-end demo wires without the full built-in batch processor, which is a later-phase component.
///
/// The transform is a property of the factory: it maps one model batch of signal `M` to a replacement
/// batch, or returns `null` to drop the batch entirely (the filter and drop-empty cases). Because the
/// domain batches are immutable records, the transform produces a new batch rather than mutating its
/// input, so the processor declares itself non-mutating and needs no fan-out copy.
///
/// @param <M> the model batch type this processor transforms
public final class FunctionProcessorFactory<M> implements ProcessorFactory<FunctionProcessorConfig> {

    private final String type;

    private final SignalDescriptor<M> signal;

    private final UnaryOperator<@Nullable M> transform;

    /// Builds a function processor factory for `signal`, applying `transform` to each batch.
    ///
    /// @param type      the component type name
    /// @param signal    the signal this processor operates on
    /// @param transform the batch transform, returning `null` to drop the batch
    public FunctionProcessorFactory(String type, SignalDescriptor<M> signal, UnaryOperator<@Nullable M> transform) {
        this.type = type;
        this.signal = signal;
        this.transform = transform;
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.PROCESSOR;
    }

    @Override
    public StabilityLevel stability() {
        return StabilityLevel.DEVELOPMENT;
    }

    @Override
    public FunctionProcessorConfig defaultConfig() {
        return FunctionProcessorConfig.defaults();
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public <S extends TelemetryPayload> Processor<S> create(
            CreateContext ctx, FunctionProcessorConfig config, SignalDescriptor<S> reqSignal, SignalConsumer<S> next) {
        if (!reqSignal.equals(signal)) {
            throw new IllegalArgumentException(
                    "processor " + type + " operates on " + signal + " but was created for " + reqSignal);
        }
        var processor = new FunctionProcessor<>(ctx.id(), signal, transform, payloadConsumer(next));
        return castProcessor(processor);
    }

    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> payloadConsumer(SignalConsumer<?> next) {
        return (SignalConsumer<TelemetryPayload>) next;
    }

    @SuppressWarnings("unchecked")
    private static <S extends TelemetryPayload> Processor<S> castProcessor(Processor<TelemetryPayload> processor) {
        return (Processor<S>) (Processor<?>) processor;
    }

    /// The processor component: it unwraps the model batch, applies the transform, and either drops
    /// the batch (a null transform result, reported as accepted with nothing forwarded) or wraps the
    /// transformed batch and forwards it to the next consumer.
    private static final class FunctionProcessor<M> implements Processor<TelemetryPayload> {

        private final SignalDescriptor<M> signal;

        private final UnaryOperator<@Nullable M> transform;

        private final SignalConsumer<TelemetryPayload> next;

        private final Provenance provenance;

        FunctionProcessor(
                ComponentId id,
                SignalDescriptor<M> signal,
                UnaryOperator<@Nullable M> transform,
                SignalConsumer<TelemetryPayload> next) {
            this.signal = signal;
            this.transform = transform;
            this.next = next;
            this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
            // Read the domain through the interface-level DOMAIN_IMMUTABLE view, so the processor works
            // whether the upstream handed it a proto-backed payload (materialising the domain once, on
            // demand) or a domain-backed one, then emit a fresh domain payload for the transform result.
            var in = signal.batchType().cast(data.view(PayloadView.DOMAIN_IMMUTABLE));
            var transformed = transform.apply(in);
            if (transformed == null) {
                return DeliveryResult.acceptedStage(provenance);
            }
            return next.consume(context, Payloads.domain(signal, transformed));
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
