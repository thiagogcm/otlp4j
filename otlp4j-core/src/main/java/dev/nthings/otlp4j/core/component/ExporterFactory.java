package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A factory for exporters. It creates one exporter bound to a signal, the terminal consumer of a
/// pipeline. The signal is supplied at creation so the same factory can produce an exporter for any
/// signal it supports; the multi-signal facet exporter over one shared client is the wrapped
/// transport client presented through per-signal exporters.
///
/// @param <C> the typed configuration this factory accepts
public interface ExporterFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one exporter for `signal`.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param config the exporter configuration
    /// @param signal the signal this exporter operates on
    /// @param <T>    the payload type
    /// @return the exporter
    <T extends TelemetryPayload> Exporter<T> create(CreateContext ctx, C config, SignalDescriptor<T> signal);

    /// Builds a terminal exporter of type `type` whose per-signal facets each delegate every consume
    /// to `terminal`, with a no-op lifecycle. This collapses the factory-plus-exporter boilerplate a
    /// trivial in-memory or programmed terminal otherwise costs to a single call: the whole-payload
    /// sink, the accept or reject decision, and any asynchrony live in the one lambda, which receives
    /// the facet's [Provenance] so it can stamp its result.
    ///
    /// @param type     the component type name
    /// @param terminal the per-invocation delivery logic, one shared instance across every signal facet
    /// @return an exporter factory over the no-op configuration
    static ExporterFactory<ComponentConfig.None> of(String type, Terminal terminal) {
        return new LambdaExporterFactory(type, terminal);
    }

    /// The per-invocation delivery logic an [#of] exporter runs: it consumes one payload and completes
    /// with a [DeliveryResult] stamped with the facet's `provenance`.
    @FunctionalInterface
    interface Terminal {

        /// Consumes one payload for the facet identified by `provenance`.
        ///
        /// @param context    the per-invocation delivery context
        /// @param data       the payload to consume
        /// @param provenance the consuming facet's provenance, to stamp the result
        /// @return a stage completing with the delivery result
        CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data, Provenance provenance);
    }

    /// The [#of] factory: it captures the terminal logic and builds one non-mutating facet per signal,
    /// each stamped with its own provenance.
    final class LambdaExporterFactory implements ExporterFactory<ComponentConfig.None> {

        private final String type;
        private final Terminal terminal;

        private LambdaExporterFactory(String type, Terminal terminal) {
            if (type == null || terminal == null) {
                throw new IllegalArgumentException("type and terminal must be non-null");
            }
            this.type = type;
            this.terminal = terminal;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
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
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, ComponentConfig.None config, SignalDescriptor<T> signal) {
            return new LambdaExporter<>(new Provenance(ctx.id(), ComponentKind.EXPORTER, signal), terminal);
        }
    }

    /// The [#of] exporter facet: it delegates every consume to the shared terminal logic with its own
    /// provenance and has a no-op lifecycle.
    ///
    /// @param <T> the payload type this facet terminates
    final class LambdaExporter<T extends TelemetryPayload> implements Exporter<T> {

        private final Provenance provenance;
        private final Terminal terminal;

        private LambdaExporter(Provenance provenance, Terminal terminal) {
            this.provenance = provenance;
            this.terminal = terminal;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            return terminal.consume(context, data, provenance);
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
