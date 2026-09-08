package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Pipeline;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Embedded pipeline demo")
class EmbeddedPipelineDemoTest {

    @Test
    @DisplayName("filters and maps a typed domain batch before exporting it")
    void transformsTypedBatch() {
        var exported = new AtomicReference<TracesData>();
        var sink = ExporterFactory.of("capture", (context, payload, provenance) -> {
            if (payload.data() instanceof TracesData traces) {
                exported.set(traces);
            }
            return DeliveryResult.acceptedStage(provenance);
        });

        try (var pipeline = Pipeline.builder(TRACES)
                .filter(batch -> batch.spanCount() > 0)
                .map(EmbeddedPipelineDemo::redactEnduserId)
                .to(sink)
                .build()) {
            pipeline.start();
            var result = pipeline.submit(EmbeddedPipelineDemo.sampleTraces()).toCompletableFuture().join();
            pipeline.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();

            assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        }

        assertThat(exported.get()).isNotNull();
        assertThat(exported.get().spans().getFirst().attributes().get("enduser.id"))
                .isEqualTo(AttributeValue.of("***"));
    }

    @Test
    @DisplayName("drops an empty typed batch without invoking the exporter")
    void dropsEmptyBatch() {
        var exported = new AtomicReference<TracesData>();
        var sink = ExporterFactory.of("capture", (context, payload, provenance) -> {
            if (payload.data() instanceof TracesData traces) {
                exported.set(traces);
            }
            return DeliveryResult.acceptedStage(provenance);
        });

        try (var pipeline = Pipeline.builder(TRACES)
                .filter(batch -> batch.spanCount() > 0)
                .to(sink)
                .build()) {
            pipeline.start();
            var result = pipeline.submit(new TracesData(List.of())).toCompletableFuture().join();
            pipeline.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();

            assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        }

        assertThat(exported.get()).isNull();
    }
}
