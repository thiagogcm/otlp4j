package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// An owned, typed in-process pipeline backed by the gateway runtime.
/// Buffering is explicit: [Builder#via] adds processors such as the bounded batch processor.
/// Acceptance at a buffer transfers ownership; [#flush] reports subsequent delivery losses.
/// @param <T> the domain batch type
public final class Pipeline<T> implements TelemetryInput<T>, AutoCloseable {
    private final GatewayRuntime runtime;
    private final TelemetryInput<T> input;

    private Pipeline(GatewayRuntime runtime, InputHandle<T> handle) {
        this.runtime = runtime;
        this.input = runtime.input(handle);
    }

    public static <T> Builder<T> builder(SignalDescriptor<T> signal) { return new Builder<>(signal); }

    public void start() { runtime.start(); }

    @Override public SignalDescriptor<T> signal() { return input.signal(); }
    @Override public CompletionStage<DeliveryResult> submit(T batch) { return input.submit(batch); }
    @Override public CompletionStage<DeliveryResult> submit(T batch, ConsumeContext context) {
        return input.submit(batch, context);
    }
    @Override public CompletionStage<FlushReport> flush(Duration timeout) { return runtime.flush(timeout); }
    public CompletionStage<Void> shutdown(Duration timeout) { return runtime.shutdown(timeout); }
    @Override public void close() { runtime.close(); }

    /// A reusable definition. Each build compiles a fresh graph and creates independent components.
    /// @param <T> the domain batch type
    public static final class Builder<T> {
        private final SignalDescriptor<T> signal;
        private final GatewayBuilder gateway = Gateway.builder();
        private final InputHandle<T> input;
        private final List<ProcessorHandle> processors = new ArrayList<>();
        private final List<ExporterHandle> exporters = new ArrayList<>();
        private boolean built;

        private Builder(SignalDescriptor<T> signal) {
            this.signal = Objects.requireNonNull(signal, "signal");
            input = gateway.input(ComponentId.parse("input"), signal);
        }

        public Builder<T> filter(Predicate<? super T> predicate) {
            Objects.requireNonNull(predicate, "predicate");
            return via(ProcessorFactory.filter("filter", signal, predicate::test));
        }

        public Builder<T> map(UnaryOperator<T> transform) {
            return via(ProcessorFactory.map("map", signal, Objects.requireNonNull(transform, "transform")));
        }

        public <C extends ComponentConfig> Builder<T> via(ProcessorFactory<C> factory) {
            return via(factory, factory.defaultConfig());
        }

        public <C extends ComponentConfig> Builder<T> via(ProcessorFactory<C> factory, C config) {
            checkMutable();
            processors.add(gateway.processor(ComponentId.parse(factory.type() + "/" + processors.size()), factory, config));
            return this;
        }

        public <C extends ComponentConfig> Builder<T> to(ExporterFactory<C> factory) {
            return to(factory, factory.defaultConfig());
        }

        public <C extends ComponentConfig> Builder<T> to(ExporterFactory<C> factory, C config) {
            checkMutable();
            exporters.add(gateway.exporter(ComponentId.parse(factory.type() + "/" + exporters.size()), factory, config));
            return this;
        }

        public Builder<T> service(ServiceOptions options) {
            checkMutable();
            gateway.service(options);
            return this;
        }

        public Pipeline<T> build() {
            if (exporters.isEmpty()) {
                // Checked here so the failure names the pipeline step to add, and so the definition
                // is not frozen without its exporter.
                throw new IllegalStateException(
                        "pipeline has no exporter; add at least one with to(...) before build()");
            }
            if (!built) {
                gateway.pipeline(PipelineId.of(signal), p -> p.from(input)
                        .via(processors.toArray(ProcessorHandle[]::new))
                        .to(exporters.toArray(ExporterHandle[]::new)));
                built = true;
            }
            return new Pipeline<>(gateway.build(), input);
        }

        private void checkMutable() {
            if (built) { throw new IllegalStateException("pipeline definition is already built"); }
        }
    }
}
