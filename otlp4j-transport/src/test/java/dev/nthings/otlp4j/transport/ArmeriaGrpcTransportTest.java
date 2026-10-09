package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.LOGS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.PROFILES;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.transport.Compression;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/// Black-box OTLP/gRPC tests over a real Armeria server on an ephemeral port, driven by the Armeria
/// gRPC client, asserting the corrected gRPC status mapping.
@Timeout(60)
@DisplayName("Armeria OTLP/gRPC transport")
class ArmeriaGrpcTransportTest {

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

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
                .route(TRACES, Consumers.partial(1L, "one span rejected")));
        var result = exporterTo(receiver)
                .export(TRACES, Fixtures.traceData(Fixtures.span("kept", Span.Kind.INTERNAL), Fixtures.span("rejected", Span.Kind.INTERNAL)))
                .toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Partial.class);
        assertThat(((DeliveryResult.Partial) result).rejectedItems()).isEqualTo(1L);
    }

    @DisplayName("bounds a client-requested deadline to the configured cap and honours a shorter one")
    @Test
    void boundsClientRequestedDeadline() {
        var seen = new AtomicReference<Deadline>();
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .maxClientTimeout(Duration.ofSeconds(5))
                .route(TRACES, (context, payload) -> {
                    seen.set(context.deadline());
                    return CompletableFuture.completedFuture(DeliveryResult.accepted(Consumers.PROV));
                }));
        var clock = Clock.systemUTC();

        assertThat(exporterTo(receiver, Duration.ofHours(1))
                        .export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get().remaining(clock)).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(5));

        assertThat(exporterTo(receiver, Duration.ofSeconds(2))
                        .export(TRACES, TransportFixtures.richTraceData()).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get().remaining(clock)).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(2));
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
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);
        var sent = TransportFixtures.richTraceData();

        assertThat(exporter.export(TRACES, sent).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received.get()).isEqualTo(sent);
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
        cleanup.receiver(receiver);
        return receiver.start();
    }

    private OtlpExporter exporterTo(OtlpReceiver receiver) {
        return exporterTo(receiver, Duration.ofSeconds(5));
    }

    private OtlpExporter exporterTo(OtlpReceiver receiver, Duration timeout) {
        var exporter = OtlpExporter.grpcBuilder()
                .endpoint("localhost", receiver.port())
                .timeout(timeout)
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), false))
                .build().start();
        cleanup.client(exporter);
        return exporter;
    }
}
