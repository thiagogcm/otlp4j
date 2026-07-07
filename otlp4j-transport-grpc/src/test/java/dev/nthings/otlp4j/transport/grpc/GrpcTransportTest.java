package dev.nthings.otlp4j.transport.grpc;

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
        var receiver = startReceiver(OtlpGrpcReceiver.builder().route(TRACES, Consumers.record(TRACES, received)));
        var sent = TransportFixtures.richTraceData();

        var result = exporterTo(receiver).export(TRACES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips rich MetricsData losslessly")
    @Test
    void roundTripsRichMetricsDataLosslessly() {
        var received = new AtomicReference<MetricsData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().route(METRICS, Consumers.record(METRICS, received)));
        var sent = TransportFixtures.richMetricsData();

        var result = exporterTo(receiver).export(METRICS, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips rich LogsData losslessly")
    @Test
    void roundTripsRichLogsDataLosslessly() {
        var received = new AtomicReference<LogsData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().route(LOGS, Consumers.record(LOGS, received)));
        var sent = TransportFixtures.richLogsData();

        var result = exporterTo(receiver).export(LOGS, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("Round-trips ProfilesData")
    @Test
    void roundTripsProfilesData() {
        var received = new AtomicReference<ProfilesData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder().route(PROFILES, Consumers.record(PROFILES, received)));
        var sent = TransportFixtures.profilesData();

        var result = exporterTo(receiver).export(PROFILES, sent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        // Opaque passthrough: scalar metadata round-trips; receiver captures wire bytes as
        // rawProfile, so a scalar-built fixture is not record-equal after the wire - compare
        // modeled fields and assert the payload was kept.
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
                .route(TRACES, Consumers.partial(2L, "2 spans malformed")));
        var metricsReceiver = startReceiver(OtlpGrpcReceiver.builder()
                .route(METRICS, Consumers.partial(5L, "5 data points rejected")));
        var logsReceiver = startReceiver(OtlpGrpcReceiver.builder()
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

    @DisplayName("Translates a throwing consumer into a transport error")
    @Test
    void translatesAThrowingConsumerIntoATransportError() {
        var receiver = startReceiver(OtlpGrpcReceiver.builder()
                .route(TRACES, Consumers.throwing(new IllegalStateException("handler boom"))));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL));

        assertThatThrownBy(() -> exporter.export(TRACES, traces).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("handler boom");
    }

    @DisplayName("An unrouted signal surfaces as retryable UNAVAILABLE")
    @Test
    void unroutedSignalSurfacesAsUnavailable() {
        var receiver = startReceiver(OtlpGrpcReceiver.builder());
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("UNAVAILABLE")
                .hasMessageContaining("no consumer attached for TracesData");
    }

    @DisplayName("Rejects starting a receiver twice")
    @Test
    void rejectsStartingAReceiverTwice() {
        var receiver = startReceiver(OtlpGrpcReceiver.builder().route(TRACES, Consumers.accepting()));

        assertThatThrownBy(receiver::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @DisplayName("Receiver port is zero before start")
    @Test
    void portIsZeroBeforeStart() {
        var receiver = OtlpGrpcReceiver.builder()
                .ephemeralPort()
                .route(TRACES, Consumers.accepting())
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
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder()
                .setMaxInboundMessageSizeBytes(8 * 1024 * 1024)
                .setMaxConcurrentCallsPerConnection(64)
                .setHandshakeTimeout(Duration.ofSeconds(10))
                .route(TRACES, Consumers.record(TRACES, received)));
        var exporter = OtlpGrpcExporter.builder()
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

    @DisplayName("Admission control rejects at capacity with UNAVAILABLE and pushback, then recovers")
    @Test
    void admissionControlRejectsAtCapacityThenRecovers() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var gate = new CompletableFuture<DeliveryResult>();
        var receiver = startReceiver(OtlpGrpcReceiver.builder()
                .setMaxInFlightRequests(1)
                .setThrottleRetryAfter(Duration.ofSeconds(3))
                .route(TRACES, Consumers.gated(entered::countDown, gate)));
        var exporter = exporterTo(receiver);
        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));

        // First export occupies the single admission slot until the gate opens.
        var first = exporter.export(TRACES, batch).toCompletableFuture();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        // Second export is rejected at admission: UNAVAILABLE "receiver at capacity" with the
        // configured pushback trailer (retries are disabled by exporterTo).
        var second = exporter.export(TRACES, batch).toCompletableFuture();
        assertThatThrownBy(second::join)
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> {
                    var status = io.grpc.Status.fromThrowable(e.getCause());
                    assertThat(status.getCode()).isEqualTo(io.grpc.Status.Code.UNAVAILABLE);
                    assertThat(status.getDescription()).contains("receiver at capacity");
                    var trailers = io.grpc.Status.trailersFromThrowable(e.getCause());
                    assertThat(trailers).isNotNull();
                    assertThat(trailers.get(io.grpc.Metadata.Key.of(
                            "grpc-retry-pushback-ms", io.grpc.Metadata.ASCII_STRING_MARSHALLER)))
                            .isEqualTo("3000");
                });

        // Releasing the slot lets both the held and a fresh export complete.
        gate.complete(DeliveryResult.accepted(Consumers.PROV));
        assertThat(first.join()).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(exporter.export(TRACES, batch).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    private ServerReceiver startReceiver(OtlpGrpcReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build();
        receivers.add(receiver);
        return receiver.start();
    }

    private ClientExporter exporterTo(ServerReceiver receiver) {
        // These tests exercise request/response mechanics and error mapping, not retry; disable
        // retries so a retryable rejection surfaces immediately rather than being retried to the
        // deadline. Retry behaviour has dedicated coverage in TransportConfigTest.
        var exporter = OtlpGrpcExporter.builder()
                .setEndpoint("localhost", receiver.port())
                .setTimeout(Duration.ofSeconds(5))
                .setRetryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), true))
                .build();
        closeables.add(exporter);
        return exporter;
    }
}
