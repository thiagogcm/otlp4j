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
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.grpc.Status;
import com.linecorp.armeria.client.grpc.GrpcClients;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
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
        var profiles = new AtomicReference<ProfilesData>();
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(TRACES, Consumers.record(TRACES, traces))
                .route(METRICS, Consumers.record(METRICS, metrics))
                .route(LOGS, Consumers.record(LOGS, logs))
                .route(PROFILES, Consumers.record(PROFILES, profiles)));
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
        var sentProfiles = TransportFixtures.profilesData();
        assertThat(exporter.export(PROFILES, sentProfiles).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);

        assertThat(traces.get()).isEqualTo(sentTraces);
        assertThat(metrics.get()).isEqualTo(sentMetrics);
        assertThat(logs.get()).isEqualTo(sentLogs);
        assertThat(profiles.get()).isEqualTo(sentProfiles);
    }

    @DisplayName("a dangling dictionary reference is the sender's error")
    @Test
    void danglingReferenceYieldsInvalidArgument() {
        var receiver = startReceiver(OtlpReceiver.grpcBuilder()
                .route(PROFILES, Consumers.record(PROFILES, new AtomicReference<>())));
        var stub = GrpcClients.newClient(
                "gproto+http://localhost:" + receiver.port() + "/", ProfilesServiceGrpc.ProfilesServiceBlockingStub.class);

        assertThatThrownBy(() -> stub.export(ArmeriaHttpWireTest.danglingProfilesRequest()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @DisplayName("a consumer that throws an Error gives its admission permit back")
    @Test
    void aFatalConsumerErrorReleasesItsAdmissionPermit() {
        var throwing = new AtomicBoolean(true);
        var consumer = new SignalConsumer<TracesData>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
                if (throwing.getAndSet(false)) {
                    throw new StackOverflowError("simulated");
                }
                return DeliveryResult.acceptedStage(new Provenance(
                        ComponentId.parse("test"), ComponentKind.EXPORTER, TRACES));
            }
        };
        var receiver = startReceiver(OtlpReceiver.grpcBuilder().maxInFlightRequests(1).route(TRACES, consumer));
        var exporter = exporterTo(receiver);
        var traces = Fixtures.traceData(Fixtures.span("op", Span.Kind.SERVER));

        assertThat(exporter.export(TRACES, traces).toCompletableFuture().handle((result, failure) -> failure).join())
                .isNotNull();

        assertThat(exporter.export(TRACES, traces).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
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
