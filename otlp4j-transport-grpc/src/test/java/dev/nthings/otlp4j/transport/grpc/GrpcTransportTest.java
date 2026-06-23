package dev.nthings.otlp4j.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Black-box OTLP/gRPC transport tests over real servers on ephemeral ports.
@Timeout(30)
@DisplayName("OTLP/gRPC transport")
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

    @DisplayName("Round-trips rich TraceData losslessly")
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

    @DisplayName("Round-trips rich MetricsData losslessly")
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

    @DisplayName("Round-trips rich LogsData losslessly")
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

    @DisplayName("Round-trips ProfilesData")
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
        // Opaque passthrough: the scalar metadata round-trips, and the receiver additionally captures
        // the wire bytes as rawProfile (the lossless forwarding payload), so a scalar-built fixture is
        // not record-equal after the wire — compare the modeled fields and assert the payload was kept.
        var sentProfile = sent.profiles().getFirst();
        var recvProfile = received.get().profiles().getFirst();
        assertThat(recvProfile.profileId()).isEqualTo(sentProfile.profileId());
        assertThat(recvProfile.timeUnixNano()).isEqualTo(sentProfile.timeUnixNano());
        assertThat(recvProfile.durationNanos()).isEqualTo(sentProfile.durationNanos());
        assertThat(recvProfile.period()).isEqualTo(sentProfile.period());
        assertThat(recvProfile.sampleCount()).isEqualTo(sentProfile.sampleCount());
        assertThat(recvProfile.droppedAttributesCount()).isEqualTo(sentProfile.droppedAttributesCount());
        assertThat(recvProfile.originalPayloadFormat()).isEqualTo(sentProfile.originalPayloadFormat());
        assertThat(recvProfile.rawProfile()).isNotEmpty();
    }

    @DisplayName("Propagates partial success for every signal")
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

    @DisplayName("Translates a throwing dispatcher into a transport error")
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

    @DisplayName("End-to-end receive, filter, export Pipeline keeps only SERVER spans")
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

    @DisplayName("Rejects starting a receiver twice")
    @Test
    void rejectsStartingAReceiverTwice() {
        var receiver = startReceiver(OtlpGrpcReceiver.builder()
                .onTraces(t -> ConsumeResult.acceptedStage()));

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @DisplayName("Receiver port is zero before start")
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

    @DisplayName("Facade builder convenience knobs (compression, retry, receiver hardening) wire through")
    @Test
    void facadeBuilderConvenienceKnobsRoundTrip() {
        var received = new AtomicReference<TraceData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder()
                .maxInboundMessageSizeBytes(8 * 1024 * 1024)
                .maxConcurrentCallsPerConnection(64)
                .handshakeTimeout(Duration.ofSeconds(10))
                .onTraces(traces -> {
                    received.set(traces);
                    return ConsumeResult.acceptedStage();
                }));
        var exporter = OtlpGrpcExporter.builder()
                .endpoint("localhost", receiver.port())
                .compression(Compression.GZIP)
                .retry(RetryPolicy.exponential(3, Duration.ofMillis(50), Duration.ofSeconds(1)))
                .timeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
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
