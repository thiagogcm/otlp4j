package dev.nthings.otlp4j.core.delivery;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// Fixtures for the delivery queue tests: a payload of a given item count, a consume context, and a
/// programmable downstream that records what it receives and can be gated on a latch.
final class DeliverySupport {

    static final Provenance PROV =
            new Provenance(ComponentId.parse("queue"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);
    static final ConsumeContext CTX = ConsumeContext.of(
            ClientMetadata.empty(), Deadline.after(Duration.ofSeconds(30), Clock.systemUTC()));
    static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

    private DeliverySupport() {}

    static Payload<TracesData> traces(int spanCount) {
        var spans = new ArrayList<Span>();
        for (var i = 0; i < spanCount; i++) {
            spans.add(Span.builder().name("span-" + i).build());
        }
        return Payload.of(
                SignalDescriptor.TRACES,
                TracesData.of(Resource.EMPTY, InstrumentationScope.of("lib"), spans));
    }

    /// A downstream that records each attempt, optionally blocks on a latch, and answers with a
    /// programmable result that may depend on the attempt number.
    static final class Recording implements SignalConsumer<TracesData> {

        private final List<Payload<TracesData>> received = new CopyOnWriteArrayList<>();
        private final List<Long> attemptTimes = new CopyOnWriteArrayList<>();
        private final AtomicInteger completions = new AtomicInteger();
        private volatile @Nullable CountDownLatch gate;
        private volatile Function<Integer, DeliveryResult> answer = attempt -> DeliveryResult.accepted(PROV);

        /// Answers every attempt with `result`.
        void always(DeliveryResult result) {
            this.answer = attempt -> result;
        }

        /// Answers attempt `n` (starting at 1) with `answer.apply(n)`.
        void byAttempt(Function<Integer, DeliveryResult> answer) {
            this.answer = answer;
        }

        void gate(@Nullable CountDownLatch latch) {
            this.gate = latch;
        }

        List<Payload<TracesData>> received() {
            return List.copyOf(received);
        }

        /// `System.nanoTime()` at the start of each attempt.
        List<Long> attemptTimes() {
            return List.copyOf(attemptTimes);
        }

        long totalItems() {
            return received.stream().mapToLong(Payload::itemCount).sum();
        }

        int completions() {
            return completions.get();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
            attemptTimes.add(System.nanoTime());
            received.add(data);
            var attempt = received.size();
            var latch = gate;
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            var result = answer.apply(attempt);
            completions.incrementAndGet();
            return CompletableFuture.completedFuture(result);
        }
    }
}
