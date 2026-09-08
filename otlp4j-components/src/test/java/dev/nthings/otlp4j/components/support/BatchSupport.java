package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Clock;

/// Shared construction helpers for batch component tests.
public final class BatchSupport {

    private BatchSupport() {}

    public static ComponentContext baseContext() {
        return new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());
    }

    public static Processor<TracesData> tracesBatch(
            String id, BatchProcessorConfig config, SignalConsumer<TracesData> next) {
        var factory = new BatchProcessorFactory();
        var ctx = new CreateContext(ComponentId.parse(id), baseContext());
        return factory.create(ctx, config, SignalDescriptor.TRACES, next);
    }

    public static Payload<TracesData> traces(int spanCount) {
        var spans = new Span[spanCount];
        for (var i = 0; i < spanCount; i++) {
            spans[i] = Fixtures.span("span-" + i, Span.Kind.INTERNAL);
        }
        return Payload.of(SignalDescriptor.TRACES, Fixtures.traceData(spans));
    }
}
