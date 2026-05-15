package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.exporter.OtlpGrpcExporter;
import dev.nthings.otlp4j.internal.TransportFixtures;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.processor.Processors;
import dev.nthings.otlp4j.receiver.OtlpReceiver;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Black-box OTLP/gRPC transport tests over real servers on ephemeral ports.
///
/// These cover domain/proto/wire round trips, partial success, failures, lifecycle, and SPI wiring.
@Timeout(30)
class GrpcTransportTest {

    private final List<OtlpReceiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // one failed close must not mask the rest of the teardown
            }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().awaitTermination(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException ignored) {
                // a receiver that never finished starting has nothing to await
            }
        }
    }

    @Test
    void roundTripsRichTraceDataLosslessly() {
        var received = new AtomicReference<TraceData>();
        var receiver = startReceiver(OtlpReceiver.builder().traceHandler(traces -> {
            received.set(traces);
            return ExportResult.success();
        }));
        var sent = TransportFixtures.richTraceData();

        var result = exporterTo(receiver).consumeTraces(sent);

        assertThat(result.isFullSuccess()).isTrue();
        assertThat(received.get())
                .as("trace telemetry must survive the proto round-trip intact")
                .isEqualTo(sent);
    }

    @Test
    void roundTripsRichMetricsDataLosslessly() {
        var received = new AtomicReference<MetricsData>();
        var receiver = startReceiver(OtlpReceiver.builder().metricsHandler(metrics -> {
            received.set(metrics);
            return ExportResult.success();
        }));
        var sent = TransportFixtures.richMetricsData();

        var result = exporterTo(receiver).consumeMetrics(sent);

        assertThat(result.isFullSuccess()).isTrue();
        assertThat(received.get())
                .as("gauge/sum/histogram/exponential/summary metrics must round-trip intact")
                .isEqualTo(sent);
    }

    @Test
    void roundTripsRichLogsDataLosslessly() {
        var received = new AtomicReference<LogsData>();
        var receiver = startReceiver(OtlpReceiver.builder().logsHandler(logs -> {
            received.set(logs);
            return ExportResult.success();
        }));
        var sent = TransportFixtures.richLogsData();

        var result = exporterTo(receiver).consumeLogs(sent);

        assertThat(result.isFullSuccess()).isTrue();
        assertThat(received.get())
                .as("log telemetry must survive the proto round-trip intact")
                .isEqualTo(sent);
    }

    @Test
    void roundTripsProfilesData() {
        var received = new AtomicReference<ProfilesData>();
        var receiver = startReceiver(OtlpReceiver.builder().profilesHandler(profiles -> {
            received.set(profiles);
            return ExportResult.success();
        }));
        var sent = TransportFixtures.profilesData();

        var result = exporterTo(receiver).consumeProfiles(sent);

        assertThat(result.isFullSuccess()).isTrue();
        assertThat(received.get())
                .as("profile metadata must round-trip intact")
                .isEqualTo(sent);
    }

    @Test
    void propagatesPartialSuccessForEverySignal() {
        var traceMessage = "2 spans malformed";
        var metricMessage = "5 data points rejected";
        var logMessage = "3 log records rejected";
        var traces = startReceiver(OtlpReceiver.builder()
                .traceHandler(t -> ExportResult.partialSuccess(2, traceMessage)));
        var metrics = startReceiver(OtlpReceiver.builder()
                .metricsHandler(m -> ExportResult.partialSuccess(5, metricMessage)));
        var logs = startReceiver(OtlpReceiver.builder()
                .logsHandler(l -> ExportResult.partialSuccess(3, logMessage)));

        var traceResult = exporterTo(traces)
                .consumeTraces(Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL)));
        var metricResult = exporterTo(metrics).consumeMetrics(new MetricsData(List.of()));
        var logResult = exporterTo(logs).consumeLogs(new LogsData(List.of()));

        assertThat(traceResult.rejectedCount()).isEqualTo(2);
        assertThat(traceResult.message()).isEqualTo(traceMessage);
        assertThat(metricResult.rejectedCount()).isEqualTo(5);
        assertThat(metricResult.message()).isEqualTo(metricMessage);
        assertThat(logResult.rejectedCount()).isEqualTo(3);
        assertThat(logResult.message()).isEqualTo(logMessage);
    }

    @Test
    void translatesAThrowingHandlerIntoATransportError() {
        var receiver = startReceiver(OtlpReceiver.builder().traceHandler(traces -> {
            throw new IllegalStateException("handler boom");
        }));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.consumeTraces(traces))
                .as("the gRPC error should carry the handler's failure message")
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("handler boom");
    }

    @Test
    void endToEndReceiveFilterExportPipeline() {
        var terminalCapture = new AtomicReference<TraceData>();
        var terminal = startReceiver(OtlpReceiver.builder().traceHandler(traces -> {
            terminalCapture.set(traces);
            return ExportResult.success();
        }));
        var pipeline = Pipeline.builder()
                .process(Processors.filterSpans(span -> span.kind() == Span.Kind.SERVER))
                .into(exporterTo(terminal));
        var gateway = startReceiver(OtlpReceiver.builder().consumer(pipeline));

        var result = exporterTo(gateway).consumeTraces(Fixtures.traceData(
                Fixtures.span("internal-op", Span.Kind.INTERNAL),
                Fixtures.span("GET /cart", Span.Kind.SERVER),
                Fixtures.span("GET /checkout", Span.Kind.SERVER)));

        assertThat(result.isFullSuccess()).isTrue();
        assertThat(terminalCapture.get())
                .as("telemetry must traverse the full receive -> filter -> export pipeline")
                .isNotNull();
        assertThat(terminalCapture.get().spans())
                .hasSize(2)
                .allMatch(span -> span.kind() == Span.Kind.SERVER);
    }

    @Test
    void rejectsStartingAReceiverTwice() {
        var receiver = startReceiver(
                OtlpReceiver.builder().traceHandler(t -> ExportResult.success()));

        assertThatThrownBy(() -> receiver.start(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server already started");
    }

    @Test
    void rejectsAPortQueryBeforeStart() {
        var receiver =
                OtlpReceiver.builder().traceHandler(t -> ExportResult.success()).build();

        assertThatThrownBy(receiver::port)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server not started");
    }

    @Test
    void declaresTheGrpcTransportProvidersAsSpiServices() {
        // The functional round-trip tests above already prove the SPI resolves end-to-end
        // (OtlpReceiver/OtlpGrpcExporter discover the transport via ServiceLoader). This test
        // pins the wiring itself: the transport module must `provides` both SPI services with
        // implementations from its internal gRPC package.
        var provides = ModuleLayer.boot()
                .findModule("dev.nthings.otlp4j.transport")
                .orElseThrow()
                .getDescriptor()
                .provides();

        assertThat(provides)
                .as("the transport module must provide both OTLP SPI services")
                .anySatisfy(p -> {
                    assertThat(p.service()).isEqualTo(OtlpServerProvider.class.getName());
                    assertThat(p.providers())
                            .allMatch(impl -> impl.startsWith("dev.nthings.otlp4j.internal."));
                })
                .anySatisfy(p -> {
                    assertThat(p.service()).isEqualTo(OtlpClientProvider.class.getName());
                    assertThat(p.providers())
                            .allMatch(impl -> impl.startsWith("dev.nthings.otlp4j.internal."));
                });
    }

    // --- helpers -------------------------------------------------------------------------------

    private OtlpReceiver startReceiver(OtlpReceiver.Builder builder) {
        // Track the receiver before start() so teardown reclaims it even if start() throws.
        var receiver = builder.build();
        receivers.add(receiver);
        try {
            return receiver.start(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private OtlpGrpcExporter exporterTo(OtlpReceiver receiver) {
        var exporter = OtlpGrpcExporter.builder()
                .endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        return exporter;
    }
}
