package dev.nthings.otlp4j.transport.spi;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Pipeline;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that exporter lifecycle is registered explicitly via `to(facet,
/// exporter)` or `owns(exporter)`, because signal facets are plain sinks
/// without lifecycle.
@DisplayName("Exporter ownership")
class ExporterOwnershipTest {

    @DisplayName("to(facet, exporter) drains the exporter on shutdown")
    @Test
    void explicitOwnerDrainsExporter() {
        var client = new RecordingOtlpClient();
        var exporter = new TestExporter(client);
        var source = new SignalSource<>(TracesData.class);

        var sub = Pipeline.from(source).to(exporter.traces(), exporter);

        assertThat(client.closes.get()).isZero();
        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(1);
    }

    @DisplayName("facets are plain sinks without lifecycle interfaces")
    @Test
    void facetsArePlainSinks() {
        var exporter = new TestExporter(new RecordingOtlpClient());
        var facet = exporter.traces();

        assertThat(facet).isNotInstanceOf(AutoCloseable.class);
    }

    @DisplayName("fan-out with explicit owners drains each exporter")
    @Test
    void fanOutWithExplicitOwnersDrainsEachExporter() {
        var clientA = new RecordingOtlpClient();
        var clientB = new RecordingOtlpClient();
        var exporterA = new TestExporter(clientA);
        var exporterB = new TestExporter(clientB);
        var source = new SignalSource<>(TracesData.class);

        var sub = Pipeline.from(source)
                .owns(exporterA)
                .owns(exporterB)
                .branch()
                .fanOut(exporterA.traces())
                .fanOut(exporterB.traces())
                .join();

        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(clientA.closes.get()).isEqualTo(1);
        assertThat(clientB.closes.get()).isEqualTo(1);
    }
}
