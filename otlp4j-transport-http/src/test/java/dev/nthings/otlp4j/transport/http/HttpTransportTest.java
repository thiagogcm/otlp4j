package dev.nthings.otlp4j.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.receiver.Receiver;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Black-box OTLP/HTTP transport tests over real servers on ephemeral ports.
@Timeout(30)
@DisplayName("OTLP/HTTP transport")
class HttpTransportTest {

    private final List<Receiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception _) {
                /* keep tearing down */ }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdownNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception _) {
                /* keep tearing down */ }
        }
    }

    @DisplayName("Round-trips rich TracesData losslessly")
    @Test
    void roundTripsRichTraceDataLosslessly() {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder().onTraces(traces -> {
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
        var receiver = startReceiver(OtlpHttpReceiver.builder().onMetrics(metrics -> {
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
        var receiver = startReceiver(OtlpHttpReceiver.builder().onLogs(logs -> {
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
        var receiver = startReceiver(OtlpHttpReceiver.builder().onProfiles(profiles -> {
            received.set(profiles);
            return ConsumeResult.acceptedStage();
        }));
        var sent = TransportFixtures.profilesData();

        var result = exporterTo(receiver).profiles().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        var sentProfile = sent.profiles().getFirst();
        var recvProfile = received.get().profiles().getFirst();
        assertThat(recvProfile.profileId()).isEqualTo(sentProfile.profileId());
        assertThat(recvProfile.sampleCount()).isEqualTo(sentProfile.sampleCount());
        assertThat(recvProfile.rawProfile()).isNotEmpty();
    }

    @DisplayName("Propagates partial success for every signal")
    @Test
    void propagatesPartialSuccessForEverySignal() {
        var traceReceiver = startReceiver(OtlpHttpReceiver.builder()
                .onTraces(t -> CompletableFuture.completedStage(
                        ConsumeResult.partial(2L, "2 spans malformed"))));
        var metricsReceiver = startReceiver(OtlpHttpReceiver.builder()
                .onMetrics(m -> CompletableFuture.completedStage(
                        ConsumeResult.partial(5L, "5 data points rejected"))));
        var logsReceiver = startReceiver(OtlpHttpReceiver.builder()
                .onLogs(l -> CompletableFuture.completedStage(
                        ConsumeResult.partial(3L, "3 log records rejected"))));

        var traceResult = exporterTo(traceReceiver).traces()
                .consume(Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL))).toCompletableFuture().join();
        var metricResult = exporterTo(metricsReceiver).metrics()
                .consume(new MetricsData(List.of())).toCompletableFuture().join();
        var logResult = exporterTo(logsReceiver).logs()
                .consume(new LogsData(List.of())).toCompletableFuture().join();

        assertThat(traceResult).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<TracesData>) traceResult).rejectedItems()).isEqualTo(2L);
        assertThat(metricResult).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<MetricsData>) metricResult).rejectedItems()).isEqualTo(5L);
        assertThat(logResult).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<LogsData>) logResult).rejectedItems()).isEqualTo(3L);
    }

    @DisplayName("A synchronously throwing dispatcher becomes a transport error (HTTP 500)")
    @Test
    void throwingDispatcherBecomesATransportError() {
        var receiver = startReceiver(OtlpHttpReceiver.builder().onTraces(traces -> {
            throw new IllegalStateException("handler boom");
        }));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.traces().consume(traces).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("handler boom");
    }

    @DisplayName("A failed-stage dispatcher becomes a transport error (HTTP 500)")
    @Test
    void failedStageDispatcherBecomesATransportError() {
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .onTraces(traces -> CompletableFuture.failedStage(new IllegalStateException("async boom"))));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.traces().consume(traces).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("async boom");
    }

    @DisplayName("A retryable whole-batch Rejected surfaces as a 503 error")
    @Test
    void rejectedWithoutCauseSurfacesAs503() {
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .onTraces(traces -> CompletableFuture.completedStage(ConsumeResult.retryable("queue full"))));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.traces()
                .consume(TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("503")
                .hasMessageContaining("queue full");
    }

    @DisplayName("A permanent whole-batch Rejected surfaces as a 500 error")
    @Test
    void rejectedWithCauseSurfacesAs500() {
        var receiver = startReceiver(OtlpHttpReceiver.builder().onTraces(traces -> CompletableFuture.completedStage(
                ConsumeResult.permanent("dropped by policy", new IllegalStateException("disallowed")))));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.traces()
                .consume(TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("500")
                .hasMessageContaining("dropped by policy");
    }

    @DisplayName("An unattached source surfaces as a retryable 503 error")
    @Test
    void unattachedSourceSurfacesAs503() {
        var receiver = startReceiver(OtlpHttpReceiver.builder());
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.traces()
                .consume(TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("503")
                .hasMessageContaining("no consumer attached for TracesData");
    }

    @DisplayName("Round-trips with gzip compression")
    @Test
    void roundTripsWithGzipCompression() {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder().onTraces(traces -> {
            received.set(traces);
            return ConsumeResult.acceptedStage();
        }));
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setCompression(Compression.GZIP)
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Retries a retryable 503 within the retry budget, then succeeds")
    @Test
    void retriesRetryableStatusThenSucceeds() {
        var calls = new AtomicInteger();
        var receiver = startReceiver(OtlpHttpReceiver.builder().onTraces(traces -> {
            if (calls.incrementAndGet() < 3) {
                return CompletableFuture.completedStage(ConsumeResult.retryable("warming up"));
            }
            return ConsumeResult.acceptedStage();
        }));
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setRetryPolicy(RetryPolicy.builder().setMaxAttempts(3).setInitialBackoff(Duration.ofMillis(1)).setMaxBackoff(Duration.ofMillis(5)).build())
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

        var result = exporter.traces()
                .consume(TransportFixtures.richTraceData()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    @DisplayName("End-to-end receive, filter, export Pipeline keeps only SERVER spans")
    @Test
    void endToEndReceiveFilterExportPipeline() {
        var terminalCapture = new AtomicReference<TracesData>();
        var terminal = startReceiver(OtlpHttpReceiver.builder().onTraces(traces -> {
            terminalCapture.set(traces);
            return ConsumeResult.acceptedStage();
        }));
        var gateway = startReceiver(OtlpHttpReceiver.builder());

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
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .onTraces(t -> ConsumeResult.acceptedStage()));

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @DisplayName("Receiver port is zero before start")
    @Test
    void portIsZeroBeforeStart() {
        var receiver = OtlpHttpReceiver.builder()
                .ephemeralPort()
                .onTraces(t -> ConsumeResult.acceptedStage())
                .build();
        try {
            assertThat(receiver.port()).isZero();
        } finally {
            receiver.shutdownNow().toCompletableFuture().join();
        }
    }

    @DisplayName("Facade builder convenience knobs (compression, retry, hardening) wire through")
    @Test
    void facadeBuilderConvenienceKnobsRoundTrip() {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .setMaxInboundMessageSizeBytes(8 * 1024 * 1024)
                .onTraces(traces -> {
                    received.set(traces);
                    return ConsumeResult.acceptedStage();
                }));
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setCompression(Compression.GZIP)
                .setRetryPolicy(RetryPolicy.builder().setMaxAttempts(3).setInitialBackoff(Duration.ofMillis(50)).setMaxBackoff(Duration.ofSeconds(1)).build())
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        var result = exporter.traces().consume(sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    private Receiver startReceiver(OtlpHttpReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpExporter exporterTo(Receiver receiver) {
        // These tests exercise request/response mechanics and error mapping, not retry; disable
        // retries so a retryable rejection surfaces immediately rather than being retried to the
        // budget. Retry behaviour has dedicated coverage in HttpTransportConfigTest.
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(5))
                .setRetryPolicy(RetryPolicy.none())
                .build();
        closeables.add(exporter);
        return exporter;
    }
}
