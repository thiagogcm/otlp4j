package dev.nthings.otlp4j.transport;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(30)
class OwnedExporterLifecycleTest {
    private static final Duration BUDGET = Duration.ofSeconds(5);
    private static final TracesData EMPTY = new TracesData(List.of());

    @RegisterExtension
    static final WireCleanup cleanup = new WireCleanup();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void flushCapturesOnlyPreviouslyDispatchedExports(boolean grpc) throws Exception {
        var firstEntered = new CompletableFuture<Void>();
        var secondEntered = new CompletableFuture<Void>();
        var first = new CompletableFuture<DeliveryResult>();
        var second = new CompletableFuture<DeliveryResult>();
        var calls = new AtomicInteger();
        var receiver = cleanup.receiver(receiver(grpc).route(TRACES, (context, payload) -> {
            if (calls.getAndIncrement() == 0) {
                firstEntered.complete(null);
                return first;
            }
            secondEntered.complete(null);
            return second;
        }).ephemeralPort().build().start());
        try (var exporter = exporter(grpc).endpoint("localhost", receiver.port())
                .timeout(BUDGET).build().start()) {
            try {
                var batch = Fixtures.traceData(Fixtures.span("direct", Span.Kind.INTERNAL));
                var firstExport = exporter.export(TRACES, batch).toCompletableFuture();
                firstEntered.get(3, TimeUnit.SECONDS);
                var flush = exporter.flush(BUDGET).toCompletableFuture();
                assertThat(flush).isNotDone();
                var secondExport = exporter.export(TRACES, EMPTY).toCompletableFuture();
                secondEntered.get(3, TimeUnit.SECONDS);
                first.complete(DeliveryResult.partial(1, "rejected directly", Consumers.PROV));
                assertThat(firstExport.get(3, TimeUnit.SECONDS)).isInstanceOf(DeliveryResult.Partial.class);
                // Unbuffered outcomes belong to export(), not a second loss report from flush().
                assertThat(flush.get(3, TimeUnit.SECONDS).isSuccess()).isTrue();
                assertThat(secondExport).isNotDone();
            } finally {
                first.complete(DeliveryResult.accepted(Consumers.PROV));
                second.complete(DeliveryResult.accepted(Consumers.PROV));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"true, context", "false, context", "true, future", "false, future",
            "true, deadline", "false, deadline"})
    void cancellationReachesTheReceiverRequest(boolean grpc, String trigger) throws Exception {
        var entered = new CompletableFuture<Void>();
        var cancelled = new CompletableFuture<Void>();
        var response = new CompletableFuture<DeliveryResult>();
        var receiver = cleanup.receiver(receiver(grpc).route(TRACES, (requestContext, payload) -> {
            requestContext.onCancel(() -> cancelled.complete(null));
            entered.complete(null);
            return response;
        }).ephemeralPort().build().start());
        try (var exporter = exporter(grpc).endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(20)).build().start()) {
            try {
                var deadline = trigger.equals("deadline")
                        ? Deadline.after(Duration.ofSeconds(2), Clock.systemUTC()) : Deadline.never();
                // The caller need not also turn its deadline into a cancellation callback.
                var context = new CallerContext(deadline);
                var delivery = exporter.export(context, TRACES, EMPTY).toCompletableFuture();
                entered.get(3, TimeUnit.SECONDS);
                switch (trigger) {
                    case "context" -> context.cancelled.complete(null);
                    case "future" -> delivery.cancel(true);
                    case "deadline" -> { }
                    default -> throw new AssertionError(trigger);
                }
                assertThatThrownBy(delivery::join).isInstanceOf(RuntimeException.class);
                cancelled.get(3, TimeUnit.SECONDS);
            } finally {
                response.complete(DeliveryResult.accepted(Consumers.PROV));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shutdownDrainsTheLastExportAndStopsAdmission(boolean grpc) throws Exception {
        var entered = new CompletableFuture<Void>();
        var response = new CompletableFuture<DeliveryResult>();
        var receiver = cleanup.receiver(
                receiver(grpc).route(TRACES, Consumers.gated(() -> entered.complete(null), response))
                        .ephemeralPort().build().start());
        try (var exporter = exporter(grpc).endpoint("localhost", receiver.port())
                .timeout(BUDGET).build().start()) {
            try {
                var delivery = exporter.export(TRACES, EMPTY).toCompletableFuture();
                entered.get(3, TimeUnit.SECONDS);
                var shutdown = exporter.shutdown(BUDGET);
                assertThat(shutdown.toCompletableFuture()).isNotDone();
                assertThatThrownBy(() -> exporter.export(TRACES, EMPTY).toCompletableFuture().join())
                        .isInstanceOf(RuntimeException.class);
                response.complete(DeliveryResult.accepted(Consumers.PROV));
                assertThat(delivery.get(3, TimeUnit.SECONDS)).isInstanceOf(DeliveryResult.Accepted.class);
                shutdown.toCompletableFuture().get(5, TimeUnit.SECONDS);
                exporter.shutdown(BUDGET).toCompletableFuture().get(5, TimeUnit.SECONDS);
            } finally {
                response.complete(DeliveryResult.accepted(Consumers.PROV));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void timedOutShutdownRemainsObservableFromClose(boolean grpc) throws Exception {
        var entered = new CompletableFuture<Void>();
        var response = new CompletableFuture<DeliveryResult>();
        var receiver = cleanup.receiver(
                receiver(grpc).route(TRACES, Consumers.gated(() -> entered.complete(null), response))
                        .ephemeralPort().build().start());
        var exporter = exporter(grpc).endpoint("localhost", receiver.port())
                .timeout(Duration.ofSeconds(20)).build().start();
        try {
            exporter.export(TRACES, EMPTY);
            entered.get(3, TimeUnit.SECONDS);
            var shutdown = exporter.shutdown(Duration.ofMillis(50));
            assertThatThrownBy(() -> shutdown.toCompletableFuture().get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            response.complete(DeliveryResult.accepted(Consumers.PROV));
            assertThatThrownBy(() -> exporter.shutdown(BUDGET).toCompletableFuture().get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThatThrownBy(exporter::close).isInstanceOf(RuntimeException.class);
        } finally {
            response.complete(DeliveryResult.accepted(Consumers.PROV));
            try {
                exporter.close();
            } catch (RuntimeException expected) {
                // The memoized timeout has already been asserted; continue fixture cleanup.
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void expiredDeadlineIsRejectedBeforeRequestPreparation(boolean grpc) {
        var preparations = new AtomicInteger();
        var config = OtlpExporterConfig.builder(4318).headers(() -> {
            preparations.incrementAndGet();
            return Map.of();
        }).build();
        // A custom context can expose its deadline independently of its cancellation flag.
        var context = new CallerContext(new Deadline(Instant.EPOCH));
        try (var exporter = exporter(grpc).config(config).build().start()) {
            assertThatThrownBy(() -> exporter.export(context, TRACES, EMPTY).toCompletableFuture().join())
                    .isInstanceOf(RuntimeException.class);
            assertThat(preparations.get()).isZero();
        }
    }

    private static OtlpReceiver.Builder receiver(boolean grpc) {
        return grpc ? OtlpReceiver.grpcBuilder() : OtlpReceiver.httpBuilder();
    }

    private static OtlpExporter.Builder exporter(boolean grpc) {
        return grpc ? OtlpExporter.grpcBuilder() : OtlpExporter.httpBuilder();
    }

    private static final class CallerContext implements ConsumeContext {
        private final Deadline deadline;
        private final CompletableFuture<Void> cancelled = new CompletableFuture<>();

        private CallerContext(Deadline deadline) { this.deadline = deadline; }
        @Override public ClientMetadata metadata() { return ClientMetadata.empty(); }
        @Override public Deadline deadline() { return deadline; }
        @Override public boolean isCancelled() { return cancelled.isDone(); }
        @Override public void onCancel(Runnable callback) { cancelled.thenRun(callback); }
    }
}
