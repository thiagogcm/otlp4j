package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A factory for exporters. It creates one exporter bound to a signal, the terminal consumer of a
/// pipeline.
///
/// @param <C> the typed configuration this factory accepts
public interface ExporterFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one exporter for `signal`.
    ///
    /// @param ctx    the create context
    /// @param config the exporter configuration
    /// @param signal the signal this exporter operates on
    /// @param <T>    the signal's domain batch type
    /// @return the exporter
    <T> Exporter<T> create(CreateContext ctx, C config, SignalDescriptor<T> signal);

    /// Builds a terminal exporter whose per-signal facets delegate to `terminal`.
    static ExporterFactory<ComponentConfig.None> of(String type, Terminal terminal) {
        return new LambdaExporterFactory(type, terminal);
    }

    /// The per-invocation delivery logic an [#of] exporter runs.
    @FunctionalInterface
    interface Terminal {

        /// Consumes one typed payload for the facet identified by `provenance`.
        CompletionStage<DeliveryResult> consume(
                ConsumeContext context, Payload<?> data, Provenance provenance);
    }

    /// The [#of] factory implementation.
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
        public <T> Exporter<T> create(
                CreateContext ctx, ComponentConfig.None config, SignalDescriptor<T> signal) {
            return new LambdaExporter<>(new Provenance(ctx.id(), ComponentKind.EXPORTER, signal), terminal);
        }
    }

    /// The [#of] exporter implementation.
    final class LambdaExporter<T> implements Exporter<T> {

        private final Provenance provenance;
        private final Terminal terminal;

        private LambdaExporter(Provenance provenance, Terminal terminal) {
            this.provenance = provenance;
            this.terminal = terminal;
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
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
