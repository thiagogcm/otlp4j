package dev.nthings.otlp4j.transport.spi;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.connector.Connectors;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.model.MetricsData;
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

    @DisplayName("a shared exporter closes only after the last subscription shuts down")
    @Test
    void sharedExporterClosesOnlyAfterLastSubscriptionReleases() {
        var client = new RecordingOtlpClient();
        var exporter = new ClientExporter(client, "test");
        var tracesSource = new SignalSource<>(TracesData.class);
        var metricsSource = new SignalSource<>(MetricsData.class);

        // Two subscriptions, each attaching a different facet of the SAME exporter.
        var subA = Pipeline.from(tracesSource).to(exporter.traces());
        var subB = Pipeline.from(metricsSource).to(exporter.metrics());

        subA.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get())
                .as("channel stays open while subscription B still holds a facet")
                .isZero();

        subB.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get())
                .as("channel closes exactly once, on the last release")
                .isEqualTo(1);
    }

    @DisplayName("a count connector cascades shutdown to its downstream exporter, no owns() needed")
    @Test
    void countConnectorDrainsDownstreamExporter() {
        var client = new RecordingOtlpClient();
        var metricsExporter = new ClientExporter(client, "test");
        var source = new SignalSource<>(TracesData.class);

        // The downstream metrics exporter is reachable ONLY through the connector.
        var sub = Pipeline.from(source).to(Connectors.spanCount(metricsExporter.metrics()));

        assertThat(client.closes.get()).isZero();
        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get())
                .as("attaching the connector drains its downstream automatically")
                .isEqualTo(1);
    }

    @DisplayName("an exporter shared by a direct facet and a connector downstream closes once, on last release")
    @Test
    void exporterSharedByFacetAndConnectorClosesOnLastRelease() {
        var client = new RecordingOtlpClient();
        var exporter = new ClientExporter(client, "test");
        var metricsSource = new SignalSource<>(MetricsData.class);
        var tracesSource = new SignalSource<>(TracesData.class);

        // Same exporter reached directly (metrics facet) and indirectly (connector downstream).
        var direct = Pipeline.from(metricsSource).to(exporter.metrics());
        var viaConnector = Pipeline.from(tracesSource).to(Connectors.spanCount(exporter.metrics()));

        viaConnector.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get())
                .as("the connector's release must not close the channel the direct subscription still holds")
                .isZero();

        direct.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(1);
    }
}
