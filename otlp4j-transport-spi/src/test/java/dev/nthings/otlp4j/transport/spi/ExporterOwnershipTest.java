package dev.nthings.otlp4j.transport.spi;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Pipeline;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that exporter facets carry the exporter's lifecycle, so a pipeline drains the exporter
/// automatically through any attached facet, with no explicit registration.
@DisplayName("Exporter ownership")
class ExporterOwnershipTest {

    @DisplayName("attaching a facet as terminal drains the exporter on shutdown")
    @Test
    void attachedFacetDrainsExporter() {
        var client = new RecordingOtlpClient();
        var exporter = new ClientExporter(client, "test");
        var source = new SignalSource<>(TracesData.class);

        var sub = Pipeline.from(source).to(exporter.traces());

        assertThat(client.closes.get()).isZero();
        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(1);
    }

    @DisplayName("facets are lifecycle-bearing views")
    @Test
    void facetsAreLifecycleBearing() {
        var exporter = new ClientExporter(new RecordingOtlpClient(), "test");
        var facet = exporter.traces();

        assertThat(facet).isInstanceOf(Lifecycle.class);
    }

    @DisplayName("fan-out drains each exporter automatically")
    @Test
    void fanOutDrainsEachExporterAutomatically() {
        var clientA = new RecordingOtlpClient();
        var clientB = new RecordingOtlpClient();
        var exporterA = new ClientExporter(clientA, "test");
        var exporterB = new ClientExporter(clientB, "test");
        var source = new SignalSource<>(TracesData.class);

        var sub = Pipeline.from(source)
                .to(FanOut.of(exporterA.traces(), exporterB.traces()));

        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(clientA.closes.get()).isEqualTo(1);
        assertThat(clientB.closes.get()).isEqualTo(1);
    }
}
