package dev.nthings.otlp4j;

import static dev.nthings.otlp4j.testing.Fixtures.span;
import static dev.nthings.otlp4j.testing.Fixtures.traceData;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/// Unit tests for the `Processor` functional interface — `andThen` composition order and the
/// `identity()` no-op.
class ProcessorTest {

    @Test
    void andThenAppliesThisProcessorBeforeTheAfterProcessor() {
        var order = new ArrayList<String>();
        var composed = recording("first", order).andThen(recording("second", order));

        composed.apply(new TelemetryConsumer() {})
                .consumeTraces(traceData(span("op", Span.Kind.INTERNAL)));

        assertThat(order)
                .as("andThen runs this processor's transform first, then after's")
                .containsExactly("first", "second");
    }

    @Test
    void identityForwardsTheConsumerUnchanged() {
        var terminal = new TelemetryConsumer() {};

        assertThat(Processor.identity().apply(terminal))
                .as("identity() returns the downstream consumer untouched")
                .isSameAs(terminal);
    }

    private static Processor recording(String label, List<String> order) {
        return next -> new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                order.add(label);
                return next.consumeTraces(traces);
            }
        };
    }
}
