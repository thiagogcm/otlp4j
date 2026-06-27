package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.exporter.AbstractOtlpExporter;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that exporter lifecycle is registered explicitly via `to(facet,
/// exporter)` or `owns(exporter)`, because signal facets are plain sinks
/// without lifecycle.
@DisplayName("Exporter ownership")
class ExporterOwnershipTest {

    static final class RecordingClient implements OtlpClient {
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public CompletionStage<ConsumeResult<TraceData>> exportTraces(TraceData t) {
            return ConsumeResult.acceptedStage();
        }

        @Override
        public CompletionStage<ConsumeResult<MetricsData>> exportMetrics(MetricsData m) {
            return ConsumeResult.acceptedStage();
        }

        @Override
        public CompletionStage<ConsumeResult<LogsData>> exportLogs(LogsData l) {
            return ConsumeResult.acceptedStage();
        }

        @Override
        public CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData p) {
            return ConsumeResult.acceptedStage();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    static final class TestExporter extends AbstractOtlpExporter {
        TestExporter(OtlpClient client) {
            super(client);
        }
    }

    @DisplayName("to(facet, exporter) drains the exporter on shutdown")
    @Test
    void explicitOwnerDrainsExporter() {
        var client = new RecordingClient();
        var exporter = new TestExporter(client);
        var source = new ManualSource<TraceData>();

        var sub = Pipeline.from(source).to(exporter.traces(), exporter);

        assertThat(client.closes.get()).isZero();
        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(1);
    }

    @DisplayName("facets are plain sinks without lifecycle interfaces")
    @Test
    void facetsArePlainSinks() {
        var exporter = new TestExporter(new RecordingClient());
        var facet = exporter.traces();

        assertThat(facet).isNotInstanceOf(AutoCloseable.class);
    }

    @DisplayName("fan-out with explicit owners drains each exporter")
    @Test
    void fanOutWithExplicitOwnersDrainsEachExporter() {
        var clientA = new RecordingClient();
        var clientB = new RecordingClient();
        var exporterA = new TestExporter(clientA);
        var exporterB = new TestExporter(clientB);
        var source = new ManualSource<TraceData>();

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
