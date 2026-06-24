package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
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

/// Verifies that the exporter facets carry the exporter's lifecycle, so the plain
/// `.to(exporter.traces())` path auto-owns the exporter without `owns(...)` or the two-arg terminal.
@DisplayName("Owned exporter facets")
class OwnedFacetLifecycleTest {

    /// A no-transport client that counts closes, so a test can prove the exporter was drained.
    static final class RecordingClient implements OtlpClient {
        final AtomicInteger closes = new AtomicInteger();

        @Override public CompletionStage<ConsumeResult<TraceData>> exportTraces(TraceData t) {
            return ConsumeResult.acceptedStage();
        }
        @Override public CompletionStage<ConsumeResult<MetricsData>> exportMetrics(MetricsData m) {
            return ConsumeResult.acceptedStage();
        }
        @Override public CompletionStage<ConsumeResult<LogsData>> exportLogs(LogsData l) {
            return ConsumeResult.acceptedStage();
        }
        @Override public CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData p) {
            return ConsumeResult.acceptedStage();
        }
        @Override public void close() {
            closes.incrementAndGet();
        }
    }

    static final class TestExporter extends AbstractOtlpExporter {
        TestExporter(OtlpClient client) {
            super(client);
        }
    }

    @DisplayName("plain .to(exporter.traces()) drains the exporter on shutdown")
    @Test
    void oneArgFacetTerminalDrainsExporter() {
        var client = new RecordingClient();
        var exporter = new TestExporter(client);
        var source = new ManualSource<TraceData>();

        var sub = Pipeline.from(source).to(exporter.traces());

        assertThat(client.closes.get()).isZero();
        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get())
                .as("the exporter behind the facet was drained without owns()/two-arg to()")
                .isEqualTo(1);
    }

    @DisplayName("a facet exposes the lifecycle interfaces that make it auto-collectable")
    @Test
    void facetCarriesLifecycleInterfaces() {
        var exporter = new TestExporter(new RecordingClient());
        var facet = exporter.traces();

        // The exact contracts the pipeline keys off: AutoCloseable (via Drainable) for collection,
        // Drainable for deadline-aware drain, Flushable for forceFlush.
        assertThat(facet).isInstanceOf(Drainable.class);
        assertThat(facet).isInstanceOf(Flushable.class);
        assertThat(facet).isInstanceOf(AutoCloseable.class);

        // forceFlush delegates to the exporter's no-op (no buffer today); assert the path completes.
        // The strong proof that the transport closes is the close()-counting tests.
        ((Flushable) facet).forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();

        var source = new ManualSource<TraceData>();
        var sub = Pipeline.from(source).to(exporter.traces());
        // forceFlush walks every owned Flushable; the auto-collected facet must be among them.
        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
    }

    @DisplayName("fan-out facet peers each drain their own exporter")
    @Test
    void fanOutFacetPeersDrainEachExporter() {
        var clientA = new RecordingClient();
        var clientB = new RecordingClient();
        var exporterA = new TestExporter(clientA);
        var exporterB = new TestExporter(clientB);
        var source = new ManualSource<TraceData>();

        var sub = Pipeline.from(source)
                .branch()
                    .fanOut(exporterA.traces())
                    .fanOut(exporterB.traces())
                .join();

        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(clientA.closes.get()).isEqualTo(1);
        assertThat(clientB.closes.get()).isEqualTo(1);
    }

    @DisplayName("combining a facet with the explicit owner overload stays idempotent")
    @Test
    void combiningWithExplicitOwnerStaysIdempotent() {
        var client = new RecordingClient();
        var exporter = new TestExporter(client);
        var source = new ManualSource<TraceData>();

        // The facet auto-owns the exporter AND it is registered explicitly: the exporter is drained
        // by both paths, but its shutdown is idempotent, so the transport is closed exactly once.
        var sub = Pipeline.from(source).to(exporter.traces(), exporter);

        sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        assertThat(client.closes.get()).isEqualTo(1);
    }
}
