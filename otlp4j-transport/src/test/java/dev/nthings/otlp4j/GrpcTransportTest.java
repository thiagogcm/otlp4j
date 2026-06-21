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
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.receiver.OtlpGrpcReceiver;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Black-box OTLP/gRPC transport tests over real servers on ephemeral ports.
@Timeout(30)
class GrpcTransportTest {

    private final List<OtlpGrpcReceiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception _) { /* keep tearing down */ }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) { /* keep tearing down */ }
        }
    }

    @Test
    void roundTripsRichTraceDataLosslessly() {
        var received = new AtomicReference<TraceData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().onTraces(traces -> {
            received.set(traces);
            return ConsumeResult.acceptedStage();
        }));
        var sent = TransportFixtures.richTraceData();

        var result = exporterTo(receiver).traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @Test
    void roundTripsRichMetricsDataLosslessly() {
        var received = new AtomicReference<MetricsData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().onMetrics(metrics -> {
            received.set(metrics);
            return ConsumeResult.acceptedStage();
        }));
        var sent = TransportFixtures.richMetricsData();

        var result = exporterTo(receiver).metrics().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @Test
    void roundTripsRichLogsDataLosslessly() {
        var received = new AtomicReference<LogsData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().onLogs(logs -> {
            received.set(logs);
            return ConsumeResult.acceptedStage();
        }));
        var sent = TransportFixtures.richLogsData();

        var result = exporterTo(receiver).logs().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @Test
    void roundTripsProfilesData() {
        var received = new AtomicReference<ProfilesData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().onProfiles(profiles -> {
            received.set(profiles);
            return ConsumeResult.acceptedStage();
        }));
        var sent = TransportFixtures.profilesData();

        var result = exporterTo(receiver).profiles().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @Test
    void propagatesPartialSuccessForEverySignal() {
        var traceReceiver = startReceiver(OtlpGrpcReceiver.builder()
                .onTraces(t -> CompletableFuture.completedStage(
                        ConsumeResult.partial(2L, "2 spans malformed"))));
        var metricsReceiver = startReceiver(OtlpGrpcReceiver.builder()
                .onMetrics(m -> CompletableFuture.completedStage(
                        ConsumeResult.partial(5L, "5 data points rejected"))));
        var logsReceiver = startReceiver(OtlpGrpcReceiver.builder()
                .onLogs(l -> CompletableFuture.completedStage(
                        ConsumeResult.partial(3L, "3 log records rejected"))));

        var traceResult = exporterTo(traceReceiver).traces()
                .consume(Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL))).toCompletableFuture().join();
        var metricResult = exporterTo(metricsReceiver).metrics()
                .consume(new MetricsData(List.of())).toCompletableFuture().join();
        var logResult = exporterTo(logsReceiver).logs()
                .consume(new LogsData(List.of())).toCompletableFuture().join();

        assertThat(traceResult).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<TraceData>) traceResult).rejectedItems()).isEqualTo(2L);
        assertThat(metricResult).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<MetricsData>) metricResult).rejectedItems()).isEqualTo(5L);
        assertThat(logResult).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<LogsData>) logResult).rejectedItems()).isEqualTo(3L);
    }

    @Test
    void translatesAThrowingDispatcherIntoATransportError() {
        var receiver = startReceiver(OtlpGrpcReceiver.builder().onTraces(traces -> {
            throw new IllegalStateException("handler boom");
        }));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.traces().consume(traces).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("handler boom");
    }

    @Test
    void endToEndReceiveFilterExportPipeline() {
        var terminalCapture = new AtomicReference<TraceData>();
        var terminal = startReceiver(OtlpGrpcReceiver.builder().onTraces(traces -> {
            terminalCapture.set(traces);
            return ConsumeResult.acceptedStage();
        }));
        var gateway = startReceiver(OtlpGrpcReceiver.builder());

        try (var terminalExporter = exporterTo(terminal)) {
            var sub = Pipeline.from(gateway.traces())
                    .transform(Transforms.keepSpansWhere(span -> span.kind() == Span.Kind.SERVER))
                    .filter(t -> !t.spans().isEmpty())
                    .to(terminalExporter.traces());

            try (var gatewayClient = exporterTo(gateway)) {
                var result = gatewayClient.traces().consume(Fixtures.traceData(
                                Fixtures.span("internal-op", Span.Kind.INTERNAL),
                                Fixtures.span("GET /cart", Span.Kind.SERVER),
                                Fixtures.span("GET /checkout", Span.Kind.SERVER)))
                        .toCompletableFuture().join();
                assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            }
            sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        }
        assertThat(terminalCapture.get()).isNotNull();
        assertThat(terminalCapture.get().spans())
                .hasSize(2)
                .allMatch(span -> span.kind() == Span.Kind.SERVER);
    }

    @Test
    void rejectsStartingAReceiverTwice() {
        var receiver = startReceiver(OtlpGrpcReceiver.builder()
                .onTraces(t -> ConsumeResult.acceptedStage()));

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @Test
    void portIsZeroBeforeStart() {
        var receiver = OtlpGrpcReceiver.builder()
                .ephemeralPort()
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        try {
            assertThat(receiver.port()).isZero();
        } finally {
            receiver.shutdownNow().toCompletableFuture().join();
        }
    }

    @Test
    void declaresTheGrpcTransportProvidersAsSpiServices() {
        var provides = ModuleLayer.boot()
                .findModule("dev.nthings.otlp4j.transport")
                .orElseThrow()
                .getDescriptor()
                .provides();

        assertThat(provides)
                .anySatisfy(p -> {
                    assertThat(p.service()).isEqualTo(OtlpServerProvider.class.getName());
                    assertThat(p.providers()).allMatch(impl -> impl.startsWith("dev.nthings.otlp4j.internal."));
                })
                .anySatisfy(p -> {
                    assertThat(p.service()).isEqualTo(OtlpClientProvider.class.getName());
                    assertThat(p.providers()).allMatch(impl -> impl.startsWith("dev.nthings.otlp4j.internal."));
                });
    }

    private OtlpGrpcReceiver startReceiver(OtlpGrpcReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpGrpcExporter exporterTo(OtlpGrpcReceiver receiver) {
        var exporter = OtlpGrpcExporter.builder()
                .endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        return exporter;
    }
}
