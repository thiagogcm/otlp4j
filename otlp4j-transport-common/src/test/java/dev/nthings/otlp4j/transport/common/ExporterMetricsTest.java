package dev.nthings.otlp4j.transport.common;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.telemetry.LoggingGatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins F11's exporter counters: `otlp4j.exporter.export.success` on an acknowledged batch,
/// `otlp4j.exporter.export.failure` on a Rejected result or a failed stage.
@DisplayName("ClientExporter self-observability")
class ExporterMetricsTest {

    private static final String SUCCESS = "otlp4j.exporter.export.success";
    private static final String FAILURE = "otlp4j.exporter.export.failure";

    /// Test client whose export outcome is derived from the provenance the facet passes in, so each
    /// result is attributed correctly while remaining fully configurable.
    private static final class FixedClient implements OtlpClient {
        private final Function<Provenance, CompletionStage<DeliveryResult>> result;

        FixedClient(Function<Provenance, CompletionStage<DeliveryResult>> result) {
            this.result = result;
        }

        @Override
        public CompletionStage<DeliveryResult> export(TelemetryPayload payload, Provenance provenance) {
            return result.apply(provenance);
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void exportOne(OtlpClient client, LoggingGatewayMetrics registry) {
        try (var exporter = new ClientExporter(client, "test", registry)) {
            exporter.export(TRACES, Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().handle((v, t) -> null).join();
        }
    }

    @DisplayName("an acknowledged export increments the success counter")
    @Test
    void acknowledgedExportCountsSuccess() {
        var registry = new LoggingGatewayMetrics();
        exportOne(new FixedClient(p -> CompletableFuture.completedStage(DeliveryResult.accepted(p))), registry);
        assertThat(registry.counterValue(SUCCESS)).isEqualTo(1);
        assertThat(registry.counterValue(FAILURE)).isZero();
    }

    @DisplayName("a Rejected result increments the failure counter")
    @Test
    void rejectedResultCountsFailure() {
        var registry = new LoggingGatewayMetrics();
        exportOne(new FixedClient(p -> CompletableFuture.completedStage(
                DeliveryResult.retryable("endpoint at capacity", p))), registry);
        assertThat(registry.counterValue(FAILURE)).isEqualTo(1);
        assertThat(registry.counterValue(SUCCESS)).isZero();
    }

    @DisplayName("a failed stage increments the failure counter")
    @Test
    void failedStageCountsFailure() {
        var registry = new LoggingGatewayMetrics();
        exportOne(new FixedClient(p -> CompletableFuture.failedFuture(new RuntimeException("boom"))), registry);
        assertThat(registry.counterValue(FAILURE)).isEqualTo(1);
        assertThat(registry.counterValue(SUCCESS)).isZero();
    }
}
