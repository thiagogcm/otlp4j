package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.LOGS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.PROFILES;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.transport.Compression;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.grpc.Metadata;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Black-box OTLP/gRPC tests over a real Armeria server on an ephemeral port, driven by the Armeria
/// gRPC client, asserting the corrected gRPC status mapping.
@Timeout(60)
@DisplayName("Armeria OTLP/gRPC transport")
class ArmeriaGrpcTransportTest {

    private final List<OtlpReceiver> receivers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
        for (var receiver : receivers) {
            try {
                receiver.shutdown(Duration.ofSeconds(5)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    @DisplayName("round-trips every signal losslessly")
    @Test
    void roundTripsEverySignal() {
        var traces = new AtomicReference<TracesData>();
        var metrics = new AtomicReference<MetricsData>();
        var logs = new AtomicReference<LogsData>();
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.record(TRACES, traces))
                .route(METRICS, Consumers.record(METRICS, metrics))
                .route(LOGS, Consumers.record(LOGS, logs))
                .route(PROFILES, Consumers.accepting()));
        var exporter = exporterTo(receiver);

        var sentTraces = TransportFixtures.richTraceData();
        var sentMetrics = TransportFixtures.richMetricsData();
        var sentLogs = TransportFixtures.richLogsData();

        assertThat(exporter.export(TRACES, sentTraces).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(exporter.export(METRICS, sentMetrics).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(exporter.export(LOGS, sentLogs).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(exporter.export(PROFILES, TransportFixtures.profilesData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);

        assertThat(traces.get()).isEqualTo(sentTraces);
        assertThat(metrics.get()).isEqualTo(sentMetrics);
        assertThat(logs.get()).isEqualTo(sentLogs);
    }

    @DisplayName("propagates partial success")
    @Test
    void propagatesPartialSuccess() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.partial(2L, "2 spans malformed")));
        var result = exporterTo(receiver)
                .export(TRACES, Fixtures.traceData(Fixtures.span("op", Span.Kind.INTERNAL)))
                .toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Partial.class);
        assertThat(((DeliveryResult.Partial) result).rejectedItems()).isEqualTo(2L);
    }

    @DisplayName("gzip compression round-trips")
    @Test
    void gzipRoundTrips() {
        var received = new AtomicReference<TracesData>();
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.record(TRACES, received)));
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .compression(Compression.GZIP)
                .timeout(Duration.ofSeconds(5))
                .build().start();
        closeables.add(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
    }

    @DisplayName("a retryable rejection maps to UNAVAILABLE")
    @Test
    void retryableRejectionMapsToUnavailable() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.throttled(Duration.ofSeconds(2), "slow down")));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @DisplayName("a permanent rejection maps to INTERNAL")
    @Test
    void permanentRejectionMapsToInternal() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.permanent("filtered")));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.INTERNAL));
    }

    @DisplayName("a throwing consumer maps to INTERNAL")
    @Test
    void throwingConsumerMapsToInternal() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.throwing(new IllegalStateException("handler boom"))));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.INTERNAL));
    }

    @DisplayName("an oversized message maps to RESOURCE_EXHAUSTED, not admission refusal")
    @Test
    void oversizedMessageMapsToResourceExhausted() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .maxInboundMessageSizeBytes(64)
                .route(TRACES, Consumers.accepting()));
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
    }

    @DisplayName("admission at capacity yields UNAVAILABLE 'receiver at capacity' with a pushback trailer")
    @Test
    void admissionAtCapacityYieldsUnavailableWithPushback() throws Exception {
        var entered = new CountDownLatch(1);
        var gate = new CompletableFuture<DeliveryResult>();
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .maxInFlightRequests(1)
                .throttleRetryAfter(Duration.ofSeconds(3))
                .route(TRACES, Consumers.gated(entered::countDown, gate)));
        var exporter = exporterTo(receiver);
        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));

        var first = exporter.export(TRACES, batch).toCompletableFuture();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        var second = exporter.export(TRACES, batch).toCompletableFuture();
        assertThatThrownBy(second::join)
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> {
                    var status = Status.fromThrowable(e.getCause());
                    assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                    assertThat(status.getDescription()).contains("receiver at capacity");
                    var trailers = Status.trailersFromThrowable(e.getCause());
                    assertThat(trailers).isNotNull();
                    assertThat(trailers.get(Metadata.Key.of(
                            "grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER)))
                            .isEqualTo("3000");
                });

        gate.complete(DeliveryResult.accepted(Consumers.PROV));
        assertThat(first.join()).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(exporter.export(TRACES, batch).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("an unrouted signal surfaces as retryable UNAVAILABLE")
    @Test
    void unroutedSignalSurfacesAsUnavailable() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder());
        var exporter = exporterTo(receiver);

        assertThatThrownBy(() -> exporter.export(TRACES, TransportFixtures.richTraceData())
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e.getCause()).getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    private OtlpReceiver startReceiver(OtlpReceiver.Builder builder) {
        var receiver = builder.ephemeralPort().build();
        receivers.add(receiver);
        return receiver.start();
    }

    private OtlpExporter exporterTo(OtlpReceiver receiver) {
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(5))
                .build().start();
        closeables.add(exporter);
        return exporter;
    }
}
