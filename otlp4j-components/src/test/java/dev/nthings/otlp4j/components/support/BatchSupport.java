package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Clock;

/// Shared construction helpers for the batch component tests: a base component context, a batch
/// processor built through its real factory over the traces signal, and traces payloads of a given
/// span count.
public final class BatchSupport {

    private BatchSupport() {
    }

    /// A base component context with no-op metrics and the system clock.
    ///
    /// @return the base context
    public static ComponentContext baseContext() {
        return new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
    }

    /// Builds a started-elsewhere batch processor for the traces signal through the real factory,
    /// forwarding to `next`. The caller starts and shuts it down.
    ///
    /// @param id     the component id
    /// @param config the batch configuration
    /// @param next   the downstream consumer
    /// @return the batch processor, not yet started
    @SuppressWarnings("unchecked")
    public static Processor<TelemetryPayload> tracesBatch(
            String id, BatchProcessorConfig config, SignalConsumer<TelemetryPayload> next) {
        var factory = new BatchProcessorFactory();
        var ctx = new CreateContext(ComponentId.parse(id), baseContext());
        var signal = (SignalDescriptor<TelemetryPayload>) (SignalDescriptor<?>) SignalDescriptor.TRACES;
        return factory.create(ctx, config, signal, next);
    }

    /// A traces payload carrying `spanCount` spans under one resource and scope.
    ///
    /// @param spanCount the number of spans
    /// @return the domain-backed traces payload
    public static TelemetryPayload traces(int spanCount) {
        var spans = new Span[spanCount];
        for (var i = 0; i < spanCount; i++) {
            spans[i] = Fixtures.span("span-" + i, Span.Kind.INTERNAL);
        }
        return Payloads.domain(SignalDescriptor.TRACES, Fixtures.traceData(spans));
    }
}
