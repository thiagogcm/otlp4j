package dev.nthings.otlp4j.transport.http;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.LOGS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.PROFILES;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.transport.common.ClientExporter;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    private final List<ServerReceiver> receivers = new ArrayList<>();
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
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(TRACES, Consumers.record(TRACES, received)));
        var sent = TransportFixtures.richTraceData();

        var result = exporterTo(receiver).export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips rich MetricsData losslessly")
    @Test
    void roundTripsRichMetricsDataLosslessly() {
        var received = new AtomicReference<MetricsData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(METRICS, Consumers.record(METRICS, received)));
        var sent = TransportFixtures.richMetricsData();

        var result = exporterTo(receiver).export(METRICS, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips rich LogsData losslessly")
    @Test
    void roundTripsRichLogsDataLosslessly() {
        var received = new AtomicReference<LogsData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(LOGS, Consumers.record(LOGS, received)));
        var sent = TransportFixtures.richLogsData();

        var result = exporterTo(receiver).export(LOGS, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips ProfilesData")
    @Test
    void roundTripsProfilesData() {
        var received = new AtomicReference<ProfilesData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(PROFILES, Consumers.record(PROFILES, received)));
        var sent = TransportFixtures.profilesData();

        var result = exporterTo(receiver).export(PROFILES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
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
                .route(TRACES, Consumers.partial(2L, "2 spans malformed")));
        var metricsReceiver = startReceiver(OtlpHttpReceiver.builder()
                .route(METRICS, Consumers.partial(5L, "5 data points rejected")));
        var logsReceiver = startReceiver(OtlpHttpReceiver.builder()
                .route(LOGS, Consumers.partial(3L, "3 log records rejected")));

        var traceResult = exporterTo(traceReceiver)
                .export(TRACES, Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL)))
                .toCompletableFuture().join();
        var metricResult = exporterTo(metricsReceiver)
                .export(METRICS, new MetricsData(List.of())).toCompletableFuture().join();
        var logResult = exporterTo(logsReceiver)
                .export(LOGS, new LogsData(List.of())).toCompletableFuture().join();

        assertThat(traceResult).isInstanceOf(DeliveryResult.Partial.class);
        assertThat(((DeliveryResult.Partial) traceResult).rejectedItems()).isEqualTo(2L);
        assertThat(metricResult).isInstanceOf(DeliveryResult.Partial.class);
        assertThat(((DeliveryResult.Partial) metricResult).rejectedItems()).isEqualTo(5L);
        assertThat(logResult).isInstanceOf(DeliveryResult.Partial.class);
        assertThat(((DeliveryResult.Partial) logResult).rejectedItems()).isEqualTo(3L);
    }

    @DisplayName("A synchronously throwing consumer becomes a transport error (HTTP 500)")
    @Test
    void throwingConsumerBecomesATransportError() {
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .route(TRACES, Consumers.throwing(new IllegalStateException("handler boom"))));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.export(TRACES, traces).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("handler boom");
    }

    @DisplayName("A failed-stage consumer becomes a transport error (HTTP 500)")
    @Test
    void failedStageConsumerBecomesATransportError() {
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .route(TRACES, Consumers.failing(new IllegalStateException("async boom"))));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.export(TRACES, traces).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("async boom");
    }

    @DisplayName("A retryable whole-batch Rejected surfaces as a 503 error")
    @Test
    void rejectedWithoutCauseSurfacesAs503() {
        var receiver = startReceiver(OtlpHttpReceiver.builder()
                .route(TRACES, Consumers.of(p -> DeliveryResult.retryable("queue full", Consumers.PROV))));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("503")
                .hasMessageContaining("queue full");
    }

    @DisplayName("A permanent whole-batch Rejected surfaces as a 500 error")
    @Test
    void rejectedWithCauseSurfacesAs500() {
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(TRACES, Consumers.of(
                p -> DeliveryResult.permanent("dropped by policy", new IllegalStateException("disallowed"), Consumers.PROV))));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("500")
                .hasMessageContaining("dropped by policy");
    }

    @DisplayName("An unrouted signal surfaces as a retryable 503 error")
    @Test
    void unroutedSignalSurfacesAs503() {
        var receiver = startReceiver(OtlpHttpReceiver.builder());
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("503")
                .hasMessageContaining("no consumer attached for TracesData");
    }

    @DisplayName("Round-trips with gzip compression")
    @Test
    void roundTripsWithGzipCompression() {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(TRACES, Consumers.record(TRACES, received)));
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setCompression(Compression.GZIP)
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        var result = exporter.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Retries a retryable 503 within the retry budget, then succeeds")
    @Test
    void retriesRetryableStatusThenSucceeds() {
        var calls = new AtomicInteger();
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(TRACES, Consumers.of(p ->
                calls.incrementAndGet() < 3
                        ? DeliveryResult.retryable("warming up", Consumers.PROV)
                        : DeliveryResult.accepted(Consumers.PROV))));
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setRetryPolicy(new RetryPolicy(3, new BackoffPolicy(Duration.ofMillis(1), 1.0, 0.0, Duration.ofMillis(1)), true))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);

        var result = exporter.export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    @DisplayName("Rejects starting a receiver twice")
    @Test
    void rejectsStartingAReceiverTwice() {
        var receiver = startReceiver(OtlpHttpReceiver.builder().route(TRACES, Consumers.accepting()));

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @DisplayName("Receiver port is zero before start")
    @Test
    void portIsZeroBeforeStart() {
        var receiver = OtlpHttpReceiver.builder()
                .ephemeralPort()
                .route(TRACES, Consumers.accepting())
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
                .route(TRACES, Consumers.record(TRACES, received)));
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setCompression(Compression.GZIP)
                .setRetryPolicy(new RetryPolicy(3, new BackoffPolicy(Duration.ofMillis(50), 1.0, 0.0, Duration.ofMillis(50)), true))
                .setTimeout(Duration.ofSeconds(5))
                .build();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        var result = exporter.export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    private ServerReceiver startReceiver(OtlpHttpReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build();
        receivers.add(receiver);
        return receiver.start();
    }

    private ClientExporter exporterTo(ServerReceiver receiver) {
        // These tests exercise request/response mechanics and error mapping, not retry; disable
        // retries so a retryable rejection surfaces immediately rather than being retried to the
        // budget. Retry behaviour has dedicated coverage in HttpTransportConfigTest.
        var exporter = OtlpHttpExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(5))
                .setRetryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), true))
                .build();
        closeables.add(exporter);
        return exporter;
    }
}
