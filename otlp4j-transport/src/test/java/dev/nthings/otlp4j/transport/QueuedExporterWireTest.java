package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.RecordingGatewayMetrics;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// The per-destination queue and redelivery of a graph OTLP exporter, over real local servers on both
/// protocols. Every destination is an [OtlpReceiver] with a programmable consumer, so the failure a
/// queue sees is a genuine 503 or UNAVAILABLE off the wire. Backoffs are milliseconds to keep the tests
/// fast; the wire-level retry is cut to two attempts so that an outage outlasts it many times over.
@Timeout(60)
class QueuedExporterWireTest {

    private static final ComponentId IN = ComponentId.parse("in");
    private static final ComponentId OUT = ComponentId.parse("out");
    private static final Duration PROMPT = Duration.ofSeconds(5);

    /// The wire attempt: two tries a few milliseconds apart, so one queue attempt costs almost nothing.
    private static final RetryPolicy SHORT_WIRE_RETRY = new RetryPolicy(
            2, new BackoffPolicy(Duration.ofMillis(5), 1.0, 0.0, Duration.ofMillis(5)), true);
    private static final RetryPolicy NO_WIRE_RETRY = new RetryPolicy(
            1, new BackoffPolicy(Duration.ofMillis(5), 1.0, 0.0, Duration.ofMillis(5)), true);

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

    private final List<CompletableFuture<DeliveryResult>> hung = new CopyOnWriteArrayList<>();

    @AfterEach
    void releaseHungRequests() {
        for (var request : hung) {
            request.complete(DeliveryResult.accepted(Consumers.PROV));
        }
    }

    private static RedeliveryPolicy redelivery(long initialMillis, long elapsedMillis) {
        return new RedeliveryPolicy(
                new BackoffPolicy(Duration.ofMillis(initialMillis), 1.0, 0.0, Duration.ofMillis(initialMillis)),
                Duration.ofMillis(elapsedMillis));
    }

    /// A destination whose answer a test changes while it runs.
    private final class Destination implements SignalConsumer<TracesData> {

        enum Mode { ACCEPT, RETRYABLE, PERMANENT, HANG, THROTTLE_ONCE, THROTTLE_FOREVER }

        volatile Mode mode = Mode.ACCEPT;
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger accepted = new AtomicInteger();
        final AtomicLong acceptedItems = new AtomicLong();
        final List<Long> callTimes = new CopyOnWriteArrayList<>();
        final OtlpReceiver receiver;

        Destination(boolean grpc, Mode mode) {
            this.mode = mode;
            var builder = grpc ? OtlpReceiver.grpcBuilder() : OtlpReceiver.httpBuilder();
            this.receiver = cleanup.receiver(builder.route(TRACES, this).ephemeralPort().build().start());
        }

        int port() {
            return receiver.port();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(
                dev.nthings.otlp4j.core.consumer.ConsumeContext context, Payload<TracesData> data) {
            var call = calls.incrementAndGet();
            callTimes.add(System.nanoTime());
            return switch (mode) {
                case ACCEPT -> {
                    accepted.incrementAndGet();
                    acceptedItems.addAndGet(data.itemCount());
                    yield CompletableFuture.completedFuture(DeliveryResult.accepted(Consumers.PROV));
                }
                case RETRYABLE -> CompletableFuture.completedFuture(DeliveryResult.retryable("outage", Consumers.PROV));
                case PERMANENT -> CompletableFuture.completedFuture(DeliveryResult.permanent("bad", Consumers.PROV));
                case THROTTLE_ONCE -> {
                    if (call == 1) {
                        yield CompletableFuture.completedFuture(
                                DeliveryResult.throttled(Duration.ofMillis(400), "slow down", Consumers.PROV));
                    }
                    accepted.incrementAndGet();
                    acceptedItems.addAndGet(data.itemCount());
                    yield CompletableFuture.completedFuture(DeliveryResult.accepted(Consumers.PROV));
                }
                case THROTTLE_FOREVER -> CompletableFuture.completedFuture(
                        DeliveryResult.throttled(Duration.ofHours(1), "come back tomorrow", Consumers.PROV));
                case HANG -> {
                    var never = new CompletableFuture<DeliveryResult>();
                    hung.add(never);
                    yield never;
                }
            };
        }
    }

    private static OtlpExporterConfig.Builder toward(boolean grpc, Destination destination) {
        var factory = grpc ? OtlpExporterFactory.grpc() : OtlpExporterFactory.http();
        return OtlpExporterConfig.defaults(factory.defaultPort()).toBuilder()
                .endpoint("127.0.0.1", destination.port())
                .retryPolicy(SHORT_WIRE_RETRY)
                .redelivery(redelivery(20, 30_000));
    }

    /// A gateway `in -> exporters`, one pipeline, started.
    private GatewayRuntime graph(
            boolean grpc, RecordingGatewayMetrics metrics, List<OtlpExporterConfig> exporters) {
        var receiverFactory = grpc ? OtlpReceiverFactory.grpc() : OtlpReceiverFactory.http();
        var exporterFactory = grpc ? OtlpExporterFactory.grpc() : OtlpExporterFactory.http();
        var gw = Gateway.builder();
        gw.receiver(IN, receiverFactory, OtlpReceiverConfig.defaults(receiverFactory.defaultPort()).withEphemeralPort());
        var ids = new ArrayList<ComponentId>();
        for (var i = 0; i < exporters.size(); i++) {
            var id = exporters.size() == 1 ? OUT : ComponentId.parse("out" + i);
            gw.exporter(id, exporterFactory, exporters.get(i));
            ids.add(id);
        }
        gw.pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(IN), List.of(), ids));
        var runtime = cleanup.runtime(gw.build(new ComponentContext(metrics, Clock.systemUTC())));
        runtime.start();
        return runtime;
    }

    private static OtlpExporter sender(boolean grpc, GatewayRuntime runtime) {
        var port = runtime.boundPort(IN).getAsInt();
        var builder = grpc ? OtlpExporter.grpcBuilder() : OtlpExporter.httpBuilder();
        return cleanup.client(builder.endpoint("127.0.0.1", port).retryPolicy(NO_WIRE_RETRY).build().start());
    }

    private static TracesData spans(int count) {
        var spans = new Span[count];
        for (var i = 0; i < count; i++) {
            spans[i] = Fixtures.span("span-" + i, Span.Kind.SERVER);
        }
        return Fixtures.traceData(spans);
    }

    private static DeliveryResult send(OtlpExporter sender, int spans) {
        return sender.export(TRACES, spans(spans)).toCompletableFuture().join();
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("an outage longer than the wire budget loses nothing: the sender is acked and the data arrives once")
    void outageSurvival(boolean grpc) throws Exception {
        var destination = new Destination(grpc, Destination.Mode.RETRYABLE);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(grpc, metrics, List.of(toward(grpc, destination).build()));
        var sender = sender(grpc, runtime);

        var begun = System.nanoTime();
        var ack = send(sender, 3);
        assertThat(ack).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(PROMPT);

        // Far more attempts than the wire budget of two: the queue, not the wire, is carrying it.
        await().atMost(PROMPT).until(() -> destination.calls.get() >= 12);
        destination.mode = Destination.Mode.ACCEPT;
        await().atMost(PROMPT).until(() -> destination.acceptedItems.get() == 3);
        Thread.sleep(300);

        assertThat(destination.accepted).hasValue(1);
        assertThat(destination.acceptedItems).hasValue(3);
        assertThat(runtime.flush(PROMPT).toCompletableFuture().join().isSuccess()).isTrue();
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isZero();
        assertThat(metrics.counterValue("otlp4j.exporter.queue.refused.items")).isZero();
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a destination that never answers does not hold back the healthy one")
    void isolationBetweenDestinations(boolean grpc) {
        var dead = new Destination(grpc, Destination.Mode.HANG);
        var healthy = new Destination(grpc, Destination.Mode.ACCEPT);
        var runtime = graph(grpc, new RecordingGatewayMetrics(), List.of(
                toward(grpc, dead).timeout(Duration.ofMillis(400)).build(),
                toward(grpc, healthy).build()));
        var sender = sender(grpc, runtime);

        var begun = System.nanoTime();
        for (var i = 0; i < 20; i++) {
            assertThat(send(sender, 2)).isInstanceOf(DeliveryResult.Accepted.class);
        }
        await().atMost(PROMPT).until(() -> healthy.acceptedItems.get() == 40);

        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(PROMPT);
        assertThat(healthy.accepted).hasValue(20);
        assertThat(dead.accepted).hasValue(0);
        assertThat(dead.calls.get()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a full queue is a retryable refusal with a hint upstream, counted as refused and never as lost")
    void fullQueueRefusesTheSender(boolean grpc) {
        var dead = new Destination(grpc, Destination.Mode.HANG);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(grpc, metrics, List.of(toward(grpc, dead)
                .retryPolicy(NO_WIRE_RETRY)
                .timeoutPolicy(new TimeoutPolicy(Duration.ofSeconds(30), Duration.ofSeconds(30)))
                .queue(new DeliveryQueuePolicy(1, 1, OverflowPolicy.FAIL))
                .build()));
        var sender = sender(grpc, runtime);

        assertThat(send(sender, 1)).isInstanceOf(DeliveryResult.Accepted.class);
        await().atMost(PROMPT).until(() -> dead.calls.get() == 1);
        assertThat(send(sender, 1)).isInstanceOf(DeliveryResult.Accepted.class);
        var refusal = send(sender, 4);

        assertThat(refusal).isInstanceOfSatisfying(DeliveryResult.Rejected.class, rejected -> {
            assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
            assertThat(rejected.retryAfter()).isNotNull();
        });
        assertThat(metrics.counterValue("otlp4j.exporter.queue.refused.items")).isEqualTo(4);
        assertThat(metrics.gaugeValue("otlp4j.exporter.queue.size")).isEqualTo(2);

        // The sender owns that retry. Once the destination answers, the flush report holds no trace of it.
        dead.mode = Destination.Mode.ACCEPT;
        hung.forEach(request -> request.complete(DeliveryResult.accepted(Consumers.PROV)));
        var report = runtime.flush(PROMPT).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isZero();
        assertThat(metrics.gaugeValue("otlp4j.exporter.queue.size")).isZero();
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("an entry that outlasts the redelivery budget is lost once, in the report and the metric")
    void budgetExpiryLosesTheEntryOnce(boolean grpc) {
        var destination = new Destination(grpc, Destination.Mode.RETRYABLE);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(grpc, metrics, List.of(
                toward(grpc, destination).redelivery(redelivery(20, 250)).build()));
        var sender = sender(grpc, runtime);

        assertThat(send(sender, 5)).isInstanceOf(DeliveryResult.Accepted.class);
        var report = runtime.flush(PROMPT).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(5);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isEqualTo(5);
        assertThat(runtime.flush(PROMPT).toCompletableFuture().join().isSuccess()).isTrue();
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isEqualTo(5);
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a permanent rejection is not retried")
    void permanentFailureIsNotRetried(boolean grpc) {
        var destination = new Destination(grpc, Destination.Mode.PERMANENT);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(grpc, metrics, List.of(toward(grpc, destination).build()));
        var sender = sender(grpc, runtime);

        assertThat(send(sender, 2)).isInstanceOf(DeliveryResult.Accepted.class);
        var report = runtime.flush(PROMPT).toCompletableFuture().join();

        assertThat(destination.calls).hasValue(1);
        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isEqualTo(2);
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a retry-after hint longer than the backoff is honoured")
    void retryAfterHintIsHonoured(boolean grpc) {
        var destination = new Destination(grpc, Destination.Mode.THROTTLE_ONCE);
        var runtime = graph(grpc, new RecordingGatewayMetrics(), List.of(
                toward(grpc, destination).retryPolicy(NO_WIRE_RETRY).redelivery(redelivery(5, 30_000)).build()));
        var sender = sender(grpc, runtime);

        assertThat(send(sender, 1)).isInstanceOf(DeliveryResult.Accepted.class);
        runtime.flush(PROMPT).toCompletableFuture().join();

        assertThat(destination.calls).hasValue(2);
        var gap = Duration.ofNanos(destination.callTimes.get(1) - destination.callTimes.get(0));
        // HTTP carries whole seconds, so its hint rounds up to at least what the server meant.
        assertThat(gap).isGreaterThanOrEqualTo(Duration.ofMillis(380));
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a hint that would pass the redelivery budget gives up at once")
    void hintBeyondTheBudgetGivesUpImmediately(boolean grpc) {
        var destination = new Destination(grpc, Destination.Mode.THROTTLE_FOREVER);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(grpc, metrics, List.of(
                toward(grpc, destination).retryPolicy(NO_WIRE_RETRY).redelivery(redelivery(5, 2_000)).build()));
        var sender = sender(grpc, runtime);

        assertThat(send(sender, 3)).isInstanceOf(DeliveryResult.Accepted.class);
        var begun = System.nanoTime();
        var report = runtime.flush(PROMPT).toCompletableFuture().join();

        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(2));
        assertThat(destination.calls).hasValue(1);
        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isEqualTo(3);
    }

    @Test
    @DisplayName("shutdown gives a waiting entry and the queued ones one attempt each and finishes in its budget")
    void shutdownAttemptsEachEntryOnce() {
        var destination = new Destination(true, Destination.Mode.RETRYABLE);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(true, metrics, List.of(toward(true, destination)
                .retryPolicy(NO_WIRE_RETRY)
                .queue(new DeliveryQueuePolicy(16, 1, OverflowPolicy.FAIL))
                .redelivery(redelivery(60_000, 600_000))
                .build()));
        var sender = sender(true, runtime);

        send(sender, 1);
        await().atMost(PROMPT).until(() -> destination.calls.get() == 1);
        send(sender, 2);
        send(sender, 4);

        var begun = System.nanoTime();
        var shutdown = runtime.shutdown(Duration.ofSeconds(10)).toCompletableFuture();
        await().atMost(Duration.ofSeconds(10)).until(shutdown::isDone);

        // 1 + 1 for the entry that was waiting, 1 for each that was queued.
        assertThat(destination.calls).hasValue(4);
        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(8));
        assertThat(shutdown).isCompletedExceptionally();
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isEqualTo(7);
    }

    @Test
    @DisplayName("shutdown finishes within its budget with a destination that never answers, and surfaces the loss")
    void shutdownWithADeadDestination() {
        var dead = new Destination(true, Destination.Mode.HANG);
        var metrics = new RecordingGatewayMetrics();
        var runtime = graph(true, metrics, List.of(toward(true, dead)
                .retryPolicy(NO_WIRE_RETRY)
                .timeoutPolicy(new TimeoutPolicy(Duration.ofSeconds(60), Duration.ofSeconds(60)))
                .queue(new DeliveryQueuePolicy(16, 1, OverflowPolicy.FAIL))
                .build()));
        var sender = sender(true, runtime);

        send(sender, 1);
        await().atMost(PROMPT).until(() -> dead.calls.get() == 1);
        send(sender, 2);

        var begun = System.nanoTime();
        var shutdown = runtime.shutdown(Duration.ofSeconds(2)).toCompletableFuture();
        await().atMost(Duration.ofSeconds(10)).until(shutdown::isDone);

        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(Duration.ofSeconds(8));
        assertThat(shutdown).isCompletedExceptionally();
        assertThat(metrics.counterValue("otlp4j.exporter.queue.lost.items")).isGreaterThanOrEqualTo(2);
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("with noQueue the exporter relays a downstream failure to the sender synchronously")
    void noQueueRelaysSynchronously(boolean grpc) {
        var destination = new Destination(grpc, Destination.Mode.RETRYABLE);
        var runtime = graph(grpc, new RecordingGatewayMetrics(), List.of(
                toward(grpc, destination).retryPolicy(NO_WIRE_RETRY).noQueue().build()));
        var sender = sender(grpc, runtime);

        var failed = send(sender, 1);
        assertThat(failed).isInstanceOfSatisfying(DeliveryResult.Rejected.class,
                rejected -> assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE));
        assertThat(destination.calls).hasValue(1);

        destination.mode = Destination.Mode.ACCEPT;
        var delivered = send(sender, 2);
        // The ack comes after the destination took it, not before.
        assertThat(delivered).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(destination.acceptedItems).hasValue(2);
    }

    @ParameterizedTest(name = "grpc={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a standalone OtlpExporter is unqueued: the caller gets the real end-to-end result of the wire attempt")
    void standaloneExporterIsUnqueued(boolean grpc) {
        var destination = new Destination(grpc, Destination.Mode.RETRYABLE);
        var builder = grpc ? OtlpExporter.grpcBuilder() : OtlpExporter.httpBuilder();
        var exporter = cleanup.client(builder
                .config(toward(grpc, destination).build())
                .build()
                .start());

        var result = exporter.export(TRACES, spans(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOfSatisfying(DeliveryResult.Rejected.class,
                rejected -> assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE));
        // Exactly the wire retry budget and nothing redelivered behind the caller's back.
        assertThat(destination.calls).hasValue(2);
    }
}
