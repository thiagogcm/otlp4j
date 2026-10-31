package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.transport.OtlpExporter;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import dev.nthings.otlp4j.transport.OtlpReceiver;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// A batch processor in front of OTLP exporters that queue, over real local servers. Each exporter
/// acknowledges on enqueue and redelivers on its own queue, so a destination that never answers does not
/// stall the healthy one behind the batch's single worker, and a batch handed to a lone exporter
/// outlives an outage longer than the exporter's short retry budget.
@Timeout(60)
class BatchedQueuedExporterTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("dest"), ComponentKind.EXPORTER, TRACES);
    private static final Duration PROMPT = Duration.ofSeconds(20);
    private static final RetryPolicy SHORT_WIRE_RETRY = new RetryPolicy(
            2, new BackoffPolicy(Duration.ofMillis(5), 1.0, 0.0, Duration.ofMillis(5)), true);

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();
    private final List<CompletableFuture<DeliveryResult>> hung = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        hung.forEach(request -> request.complete(DeliveryResult.accepted(PROV)));
        // Newest first: the sender, then the gateway, then the destinations it was exporting to.
        for (var closeable : closeables.reversed()) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }

    private enum Mode { ACCEPT, RETRYABLE, HANG }

    private final class Destination implements SignalConsumer<TracesData> {
        volatile Mode mode;
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger accepted = new AtomicInteger();
        final AtomicLong acceptedSpans = new AtomicLong();
        final OtlpReceiver receiver;

        Destination(Mode mode) {
            this.mode = mode;
            this.receiver = OtlpReceiver.grpcBuilder().route(TRACES, this).ephemeralPort().build().start();
            closeables.add(() -> receiver.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join());
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
            calls.incrementAndGet();
            switch (mode) {
                case ACCEPT -> {
                    accepted.incrementAndGet();
                    acceptedSpans.addAndGet(data.itemCount());
                    return CompletableFuture.completedFuture(DeliveryResult.accepted(PROV));
                }
                case RETRYABLE -> {
                    return CompletableFuture.completedFuture(DeliveryResult.retryable("outage", PROV));
                }
                default -> {
                    var never = new CompletableFuture<DeliveryResult>();
                    hung.add(never);
                    return never;
                }
            }
        }
    }

    private static OtlpExporterConfig toward(Destination destination, Duration timeout) {
        return OtlpExporterConfig.defaults(4317).toBuilder()
                .endpoint("127.0.0.1", destination.receiver.port())
                .retryPolicy(SHORT_WIRE_RETRY)
                .timeoutPolicy(new TimeoutPolicy(timeout, timeout))
                .redelivery(new RedeliveryPolicy(
                        new BackoffPolicy(Duration.ofMillis(20), 1.0, 0.0, Duration.ofMillis(20)),
                        Duration.ofSeconds(60)))
                .build();
    }

    /// `in -> batch -> exporters`, started, plus a sender into `in`.
    private OtlpExporter batchedGraph(List<Destination> destinations, Duration exportTimeout) {
        var receiverFactory = OtlpReceiverFactory.grpc();
        var gw = Gateway.builder();
        var in = ComponentId.parse("in");
        var batch = ComponentId.parse("batch");
        gw.receiver(in, receiverFactory, OtlpReceiverConfig.defaults(receiverFactory.defaultPort()).withEphemeralPort());
        gw.processor(batch, new BatchProcessorFactory(), new BatchProcessorConfig(
                new BatchPolicy(10, 0, 0, Duration.ofMillis(50), List.of()),
                BatchProcessorConfig.DEFAULT_NUM_CONSUMERS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS));
        var exporters = new ArrayList<ComponentId>();
        for (var i = 0; i < destinations.size(); i++) {
            var id = ComponentId.parse("out" + i);
            gw.exporter(id, OtlpExporterFactory.grpc(), toward(destinations.get(i), exportTimeout));
            exporters.add(id);
        }
        gw.pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(in), List.of(batch), exporters));
        GatewayRuntime runtime = gw.build();
        closeables.add(() -> runtime.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join());
        runtime.start();
        var sender = OtlpExporter.grpcBuilder()
                .endpoint("127.0.0.1", runtime.boundPort(in).getAsInt())
                .retryPolicy(new RetryPolicy(1, BackoffPolicy.defaults(), true))
                .build()
                .start();
        closeables.add(sender);
        return sender;
    }

    private static TracesData spans(int count) {
        var spans = new ArrayList<Span>(count);
        for (var i = 0; i < count; i++) {
            spans.add(Span.builder().name("span-" + i).kind(Span.Kind.SERVER).build());
        }
        return TracesData.of(Resource.EMPTY, InstrumentationScope.of("test"), spans);
    }

    @Test
    void aDestinationThatNeverAnswersDoesNotStallTheHealthyOneBehindABatch() {
        var dead = new Destination(Mode.HANG);
        var healthy = new Destination(Mode.ACCEPT);
        var sender = batchedGraph(List.of(dead, healthy), Duration.ofSeconds(60));

        var begun = System.nanoTime();
        for (var i = 0; i < 30; i++) {
            assertThat(sender.export(TRACES, spans(2)).toCompletableFuture().join())
                    .isInstanceOf(DeliveryResult.Accepted.class);
        }
        await().atMost(PROMPT).until(() -> healthy.acceptedSpans.get() == 60);

        // A waits out a minute per attempt, so a batch worker that waited on A would not get here.
        assertThat(Duration.ofNanos(System.nanoTime() - begun)).isLessThan(PROMPT);
        assertThat(dead.accepted).hasValue(0);
        assertThat(dead.calls.get()).isGreaterThan(0);
    }

    @Test
    void aBatchAcknowledgedBeforeAnOutageArrivesOnceAfterIt() throws Exception {
        var destination = new Destination(Mode.RETRYABLE);
        var sender = batchedGraph(List.of(destination), Duration.ofSeconds(5));

        for (var i = 0; i < 5; i++) {
            assertThat(sender.export(TRACES, spans(2)).toCompletableFuture().join())
                    .isInstanceOf(DeliveryResult.Accepted.class);
        }
        // Many times the wire budget of two attempts.
        await().atMost(PROMPT).until(() -> destination.calls.get() >= 12);
        destination.mode = Mode.ACCEPT;
        await().atMost(PROMPT).until(() -> destination.acceptedSpans.get() == 10);
        Thread.sleep(300);

        assertThat(destination.acceptedSpans).hasValue(10);
    }
}
