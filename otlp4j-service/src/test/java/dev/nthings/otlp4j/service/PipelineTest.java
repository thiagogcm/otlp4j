package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import dev.nthings.otlp4j.service.validation.GraphValidationException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class PipelineTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final TracesData EMPTY = new TracesData(List.of());
    private static final TracesData BATCH = TracesData.of(Resource.EMPTY, InstrumentationScope.of("test"),
            List.of(Span.builder().name("original").build()));

    @Test
    void directInputFiltersAndTransformsWithoutCodecOrListener() {
        var sink = new RecordingSink();
        var replacement = TracesData.of(Resource.EMPTY, InstrumentationScope.of("replacement"), BATCH.spans());
        try (var pipeline = Pipeline.builder(TRACES).filter(batch -> batch.spanCount() > 0)
                .map(batch -> replacement).to(sink).build()) {
            assertThat(pipeline.signal()).isSameAs(TRACES);
            assertThatThrownBy(() -> pipeline.submit(BATCH)).isInstanceOf(IllegalStateException.class);
            assertThat(sink.starts.get()).isZero();
            pipeline.start();
            assertThat(pipeline.submit(EMPTY).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(pipeline.submit(BATCH).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(sink.received).containsExactly(replacement);
            assertThat(pipeline.flush(TIMEOUT).toCompletableFuture().join().isSuccess()).isTrue();
            assertThatThrownBy(pipeline::start).isInstanceOf(IllegalStateException.class);
        }
        assertThat(sink.stops.get()).isEqualTo(1);
    }

    @Test
    void largeLifecycleBudgetsDoNotOverflowNanoseconds() {
        var budget = Duration.ofSeconds(Long.MAX_VALUE);
        var sink = new RecordingSink();
        try (var pipeline = Pipeline.builder(TRACES).service(new ServiceOptions(budget, budget))
                .to(sink).build()) {
            pipeline.start();
            assertThat(pipeline.submit(BATCH).toCompletableFuture().join())
                    .isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(pipeline.flush(budget).toCompletableFuture().join().isSuccess()).isTrue();
        }
        assertThat(sink.stops.get()).isEqualTo(1);
    }

    @Test
    void transformingOneFanOutBranchPreservesItsSibling() {
        var builder = Gateway.builder();
        var input = builder.input(ComponentId.parse("input"), TRACES);
        var mappedSink = new RecordingSink();
        var originalSink = new RecordingSink();
        var mapped = builder.exporter(ComponentId.parse("mapped"), mappedSink);
        var original = builder.exporter(ComponentId.parse("original"), originalSink);
        var transform = builder.processor(ComponentId.parse("map"), ProcessorFactory.map("map", TRACES, batch -> EMPTY));
        builder.pipeline(PipelineId.of(TRACES, "mapped"), p -> p.from(input).via(transform).to(mapped));
        builder.pipeline(PipelineId.of(TRACES, "original"), p -> p.from(input).to(original));
        try (var runtime = builder.build()) {
            runtime.start();
            assertThat(runtime.input(input).submit(BATCH).toCompletableFuture().join())
                    .isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(mappedSink.received).containsExactly(EMPTY);
            assertThat(originalSink.received).containsExactly(BATCH);
        }
    }

    @Test
    void nullMappingIsAnObservableRejection() {
        var sink = new RecordingSink();
        try (var pipeline = Pipeline.builder(TRACES).map(batch -> null).to(sink).build()) {
            pipeline.start();
            assertThat(pipeline.submit(BATCH).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Rejected.class);
            assertThat(sink.received).isEmpty();
        }
    }

    @Test
    void advancedInputResolvesOnlyAgainstItsOwningGraph() {
        var first = Gateway.builder();
        var firstInput = first.input(ComponentId.parse("input"), TRACES);
        var firstOut = first.exporter(ComponentId.parse("out"), new RecordingSink());
        first.pipeline(PipelineId.of(TRACES), p -> p.from(firstInput).to(firstOut));
        var second = Gateway.builder();
        var secondInput = second.input(ComponentId.parse("input"), TRACES);
        var secondOut = second.exporter(ComponentId.parse("out"), new RecordingSink());
        assertThatThrownBy(() -> second.pipeline(PipelineId.of(TRACES), p -> p.from(firstInput).to(secondOut)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another builder");
        second.pipeline(PipelineId.of(TRACES), p -> p.from(secondInput).to(secondOut));
        try (var runtime = first.build()) {
            assertThatThrownBy(() -> runtime.input(secondInput)).isInstanceOf(IllegalArgumentException.class);
            var input = runtime.input(firstInput);
            runtime.start();
            assertThat(input.submit(EMPTY).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(input.flush(TIMEOUT).toCompletableFuture().join().isSuccess()).isTrue();
        }
    }

    @Test
    void inputSignalAndDuplicateRegistrationsFailBeforeConstruction() {
        var builder = Gateway.builder();
        var input = builder.input(ComponentId.parse("input"), TRACES);
        assertThatThrownBy(() -> builder.input(input.id(), TRACES)).isInstanceOf(IllegalArgumentException.class);
        var sink = new RecordingSink();
        var output = builder.exporter(ComponentId.parse("out"), sink);
        assertThatThrownBy(() -> builder.exporter(output.id(), sink)).isInstanceOf(IllegalArgumentException.class);
        builder.pipeline(PipelineId.of(METRICS), p -> p.from(input).to(output));
        assertThatThrownBy(() -> builder.pipeline(PipelineId.of(METRICS), p -> p.from(input).to(output)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(builder::build).isInstanceOf(GraphValidationException.class);
        assertThat(sink.starts.get()).isZero();
    }

    @Test
    void repeatedBuildsHaveIndependentLifecycleAndShutdownIsIdempotent() {
        var sink = new RecordingSink();
        var definition = Pipeline.builder(TRACES).to(sink);
        var first = definition.build();
        var second = definition.build();
        assertThatThrownBy(() -> definition.map(batch -> batch)).isInstanceOf(IllegalStateException.class);
        first.start();
        second.start();
        var shutdown = first.shutdown(TIMEOUT);
        assertThat(first.shutdown(TIMEOUT) == shutdown).isTrue();
        shutdown.toCompletableFuture().join();
        assertThatThrownBy(() -> first.submit(BATCH)).isInstanceOf(IllegalStateException.class);
        assertThat(second.submit(BATCH).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
        first.close();
        second.close();
        assertThat(sink.stops.get()).isEqualTo(2);
    }

    @Test
    void shutdownBeforeStartClosesTheDefinition() {
        var sink = new RecordingSink();
        var pipeline = Pipeline.builder(TRACES).service(ServiceOptions.defaults()).to(sink, ComponentConfig.NONE).build();
        pipeline.close();
        assertThat(sink.starts.get()).isZero();
        assertThat(sink.stops.get()).isEqualTo(1);
        assertThatThrownBy(pipeline::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void inputFlushDoesNotWaitForSubmissionsMadeAfterItsBarrier() throws Exception {
        var sink = new RecordingSink();
        var firstDelivery = new CompletableFuture<DeliveryResult>();
        var secondDelivery = new CompletableFuture<DeliveryResult>();
        sink.results.add(firstDelivery);
        sink.results.add(secondDelivery);
        try (var pipeline = Pipeline.builder(TRACES).to(sink).build()) {
            pipeline.start();
            pipeline.submit(BATCH);
            var flush = pipeline.flush(TIMEOUT).toCompletableFuture();
            pipeline.submit(BATCH);
            firstDelivery.complete(accepted());
            assertThat(flush.get(2, TimeUnit.SECONDS).isSuccess()).isTrue();
            assertThat(secondDelivery).isNotDone();
            secondDelivery.complete(accepted());
        }
    }

    @Test
    void flushTimeoutDoesNotCorruptPendingSubmissionAccounting() {
        var sink = new RecordingSink();
        var delivery = new CompletableFuture<DeliveryResult>();
        sink.results.add(delivery);
        try (var pipeline = Pipeline.builder(TRACES).to(sink).build()) {
            pipeline.start();
            pipeline.submit(BATCH);
            assertThatThrownBy(() -> pipeline.flush(Duration.ofMillis(30)).toCompletableFuture().join())
                    .isInstanceOf(RuntimeException.class);
            delivery.complete(accepted());
            assertThat(pipeline.flush(TIMEOUT).toCompletableFuture().join().isSuccess()).isTrue();
        }
    }

    @Test
    void timedOutFlushRetainsLateLossWithoutOverlappingTheComponent() throws Exception {
        var sink = new RecordingSink();
        var late = new CompletableFuture<FlushReport>();
        sink.flushResults.add(late);
        try (var pipeline = Pipeline.builder(TRACES).to(sink).build()) {
            pipeline.start();
            var first = pipeline.flush(Duration.ofMillis(500));
            sink.firstFlush.get(2, TimeUnit.SECONDS);
            assertThatThrownBy(() -> first.toCompletableFuture().join()).isInstanceOf(RuntimeException.class);
            assertThat(late).isNotCancelled();
            assertThatThrownBy(() -> pipeline.flush(Duration.ofMillis(30)).toCompletableFuture().join())
                    .isInstanceOf(RuntimeException.class);
            assertThat(sink.flushes.get()).isEqualTo(1);
            late.complete(new FlushReport(2, 3, 1, new IllegalStateException("late export failure")));
            var recovered = pipeline.flush(TIMEOUT).toCompletableFuture().join();
            assertThat(recovered.rejectedItems()).isEqualTo(2);
            assertThat(recovered.droppedItems()).isEqualTo(3);
            assertThat(recovered.failedBatches()).isEqualTo(1);
            assertThat(recovered.firstFailure()).hasMessage("late export failure");
            assertThat(pipeline.flush(TIMEOUT).toCompletableFuture().join().isSuccess()).isTrue();
        }
    }

    @Test
    void shutdownSurfacesAnUnreportedLateFlushLoss() throws Exception {
        var sink = new RecordingSink();
        var late = new CompletableFuture<FlushReport>();
        sink.flushResults.add(late);
        var pipeline = Pipeline.builder(TRACES).to(sink).build();
        pipeline.start();
        var flush = pipeline.flush(Duration.ofMillis(500));
        sink.firstFlush.get(2, TimeUnit.SECONDS);
        assertThatThrownBy(() -> flush.toCompletableFuture().join()).isInstanceOf(RuntimeException.class);
        late.complete(new FlushReport(1, 0, 0, null));
        assertThatThrownBy(() -> pipeline.shutdown(TIMEOUT).toCompletableFuture().join())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(sink.stops.get()).isEqualTo(1);
    }

    @Test
    void timedOutSynchronousStartCannotOverlapRollback() throws Exception {
        var sink = new RecordingSink();
        var entered = new CompletableFuture<Void>();
        var release = new CountDownLatch(1);
        var starting = new AtomicBoolean();
        var overlap = new AtomicBoolean();
        sink.onStart = () -> {
            starting.set(true);
            entered.complete(null);
            boolean interrupted = false;
            for (;;) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException ignored) { interrupted = true; }
            }
            starting.set(false);
            if (interrupted) { Thread.currentThread().interrupt(); }
        };
        sink.onShutdown = () -> overlap.set(starting.get());
        var pipeline = Pipeline.builder(TRACES).service(new ServiceOptions(
                Duration.ofMillis(200), Duration.ofMillis(100))).to(sink).build();
        var start = new CompletableFuture<Void>();
        Thread.ofVirtual().start(() -> {
            try { pipeline.start(); start.complete(null); }
            catch (Throwable failure) { start.completeExceptionally(failure); }
        });
        try {
            entered.get(2, TimeUnit.SECONDS);
            assertThatThrownBy(() -> start.get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(GatewayLifecycleException.class);
            assertThat(sink.stops.get()).isZero();
        } finally {
            release.countDown();
        }
        sink.stopped.get(2, TimeUnit.SECONDS);
        assertThat(overlap.get()).isFalse();
        pipeline.close();
    }

    @Test
    void shutdownDeadlineDoesNotRaceAnOutstandingFlush() throws Exception {
        var sink = new RecordingSink();
        var late = new CompletableFuture<FlushReport>();
        sink.flushResults.add(late);
        var pipeline = Pipeline.builder(TRACES).to(sink).build();
        pipeline.start();
        var flush = pipeline.flush(Duration.ofMillis(500));
        sink.firstFlush.get(2, TimeUnit.SECONDS);
        assertThatThrownBy(() -> flush.toCompletableFuture().join()).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> pipeline.shutdown(Duration.ofMillis(30)).toCompletableFuture().join())
                .isInstanceOf(RuntimeException.class);
        assertThat(sink.stops.get()).isZero();
        late.complete(FlushReport.empty());
        sink.stopped.get(2, TimeUnit.SECONDS);
        assertThat(sink.stops.get()).isEqualTo(1);
    }

    @Test
    void factoryFailureCleansPreviouslyCreatedComponentsWithoutStartingThem() {
        var sink = new RecordingSink();
        var failure = new IllegalStateException("processor construction failed");
        var broken = new ProcessorFactory<ComponentConfig.None>() {
            @Override public String type() { return "broken"; }
            @Override public ComponentKind kind() { return ComponentKind.PROCESSOR; }
            @Override public ComponentConfig.None defaultConfig() { return ComponentConfig.NONE; }
            @Override public <T> Processor<T> create(CreateContext context, ComponentConfig.None config,
                                                     SignalDescriptor<T> signal, SignalConsumer<T> next) {
                throw failure;
            }
        };
        assertThatThrownBy(() -> Pipeline.builder(TRACES).via(broken).to(sink).build()).isSameAs(failure);
        assertThat(sink.starts.get()).isZero();
        assertThat(sink.stops.get()).isEqualTo(1);
    }

    @Test
    void terminalIsRequired() {
        assertThatThrownBy(() -> Pipeline.builder(TRACES).build()).isInstanceOf(GraphValidationException.class);
    }

    private static DeliveryResult accepted() {
        return DeliveryResult.accepted(new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, TRACES));
    }

    private static final class RecordingSink implements ExporterFactory<ComponentConfig.None> {
        private final List<Object> received = new CopyOnWriteArrayList<>();
        private final List<CompletableFuture<DeliveryResult>> results = new CopyOnWriteArrayList<>();
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger stops = new AtomicInteger();
        private final AtomicInteger exports = new AtomicInteger();
        private final AtomicInteger flushes = new AtomicInteger();
        private final List<CompletableFuture<FlushReport>> flushResults = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Void> firstFlush = new CompletableFuture<>();
        private final CompletableFuture<Void> stopped = new CompletableFuture<>();
        private Runnable onStart = () -> {};
        private Runnable onShutdown = () -> {};
        @Override public String type() { return "sink"; }
        @Override public ComponentKind kind() { return ComponentKind.EXPORTER; }
        @Override public ComponentConfig.None defaultConfig() { return ComponentConfig.NONE; }
        @Override public <T> Exporter<T> create(CreateContext context, ComponentConfig.None config, SignalDescriptor<T> signal) {
            return new Exporter<>() {
                @Override public CompletionStage<DeliveryResult> consume(ConsumeContext ctx, Payload<T> payload) {
                    received.add(payload.data());
                    var index = exports.getAndIncrement();
                    return index < results.size() ? results.get(index) : CompletableFuture.completedFuture(accepted());
                }
                @Override public CompletionStage<Void> start(ComponentContext ctx) {
                    starts.incrementAndGet();
                    onStart.run();
                    return CompletableFuture.completedFuture(null);
                }
                @Override public CompletionStage<FlushReport> flush(FlushContext ctx) {
                    var index = flushes.getAndIncrement();
                    firstFlush.complete(null);
                    return index < flushResults.size() ? flushResults.get(index)
                            : CompletableFuture.completedFuture(FlushReport.empty());
                }
                @Override public CompletionStage<Void> shutdown(ShutdownContext ctx) {
                    onShutdown.run();
                    stops.incrementAndGet();
                    stopped.complete(null);
                    return CompletableFuture.completedFuture(null);
                }
            };
        }
    }
}
