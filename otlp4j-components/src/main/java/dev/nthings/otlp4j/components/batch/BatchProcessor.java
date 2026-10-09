package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.components.internal.DeliveryQueue;
import dev.nthings.otlp4j.components.internal.ExactBatchSplitter;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/// A generic OTLP batch processor. Incoming submissions are first admitted into one global bounded
/// accounting domain, then accumulated by the configured metadata key. The limits include every
/// submission still buffered, queued, or in flight; no hidden waiter queue exists. The configured
/// sizer weighs each submission: a weighed submission is charged in bytes, and one the sizer weighs as
/// zero is charged by its item count. A flush captures
/// the work observed at this processor's barrier and returns a bounded report after that work reaches
/// the downstream consumer. Concurrent submissions may race with the barrier and be included in the
/// same report; an exact global exclusion requires a barrier through every buffering component.
///
/// Profiles are intentionally excluded by [BatchProcessorFactory]: distinct opaque dictionaries
/// cannot be merged without re-indexing. Profile payloads therefore remain one-to-one downstream.
public final class BatchProcessor<M> implements Processor<M> {

    private final SignalDescriptor<M> signal;
    private final int sendBatchSize;
    private final int sendBatchMaxSize;
    private final long sendBatchMaxBytes;
    private final long timeoutNanos;
    private final List<String> metadataKeys;
    private final int maxPendingSubmissions;
    private final long maxPendingBytes;
    private final long maxPendingItems;
    private final int maxPartitions;
    private final PayloadSizer sizer;
    private final SignalConsumer<M> next;
    private final Provenance provenance;
    private final DeliveryQueue<M> queue;
    private final LongSupplier nanoTime;

    private final Object lock = new Object();
    private final Map<MetadataBatchKey, Batch> buffers = new HashMap<>();
    private final AtomicReference<CompletableFuture<Void>> shutdownResult = new AtomicReference<>();
    private final AtomicReference<FailureAccumulator> failures = new AtomicReference<>(new FailureAccumulator());
    private final AtomicLong pendingSubmissions = new AtomicLong();
    private final AtomicLong pendingBytes = new AtomicLong();
    private final AtomicLong pendingUnweighedItems = new AtomicLong();

    private volatile boolean closed;
    private volatile @Nullable ScheduledExecutorService scheduler;

    BatchProcessor(
            ComponentId id,
            SignalDescriptor<M> signal,
            BatchProcessorConfig config,
            SignalConsumer<M> next,
            LongSupplier nanoTime) {
        this.signal = Objects.requireNonNull(signal, "signal");
        this.sendBatchSize = config.batch().sendBatchSize();
        this.sendBatchMaxSize = config.batch().sendBatchMaxSize();
        this.sendBatchMaxBytes = config.batch().sendBatchMaxBytes();
        this.timeoutNanos = config.batch().timeout().toNanos();
        this.metadataKeys = config.batch().metadataKeys();
        this.maxPendingSubmissions = config.maxPendingSubmissions();
        this.maxPendingBytes = config.maxPendingBytes();
        this.maxPendingItems = config.maxPendingItems();
        this.maxPartitions = config.maxPartitions();
        this.sizer = config.sizer();
        this.next = Objects.requireNonNull(next, "next");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.queue = new DeliveryQueue<>(config.delivery(), config.degradedWindow(), next, provenance, nanoTime);
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<M> data) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(data, "data");
        if (context.isCancelled()) {
            return completed(DeliveryResult.permanent("submission is cancelled", provenance));
        }
        if (context.deadline().isExpired(Clock.systemUTC())) {
            return completed(DeliveryResult.timedOut("submission deadline elapsed", provenance));
        }
        long bytes;
        long items;
        M in;
        try {
            bytes = sizer.sizeOf(data);
            if (bytes < 0) {
                throw new IllegalStateException("payload sizer returned a negative size: " + bytes);
            }
            items = data.itemCount();
            // Batching necessarily materialises the domain. Do this before admission so an
            // indivisible item that cannot satisfy the exact byte cap is rejected atomically.
            in = data.data();
        } catch (Throwable failure) {
            return completed(DeliveryResult.permanentFrom("batch materialisation", failure, provenance));
        }
        if (sendBatchMaxBytes > 0) {
            try {
                ExactBatchSplitter.split(signal, in, 0, sendBatchMaxBytes, sizer);
            } catch (Throwable failure) {
                return completed(DeliveryResult.permanentFrom("batch byte cap", failure, provenance));
            }
        }
        final MetadataBatchKey key;
        final ConsumeContext detachedContext;
        try {
            // Capture metadata and construct the detached downstream context before reserving any
            // admission capacity. A malformed context must not leave a reserved slot or a partially
            // appended batch if metadata access itself fails.
            var metadata = Objects.requireNonNull(context.metadata(), "context metadata");
            key = computeKey(metadata);
            detachedContext = ConsumeContext.of(metadata, Deadline.never());
        } catch (Throwable failure) {
            return completed(DeliveryResult.permanentFrom("batch context capture", failure, provenance));
        }
        // A submission the sizer could not weigh is charged by its items instead of its bytes.
        var unweighedItems = bytes == 0 ? items : 0;
        Batch snapshot;
        synchronized (lock) {
            if (closed) {
                return completed(DeliveryResult.shuttingDown("batch processor is shutting down", provenance));
            }
            if (!admit(key, bytes, unweighedItems)) {
                return completed(DeliveryResult.admissionRefused(
                        "batch processor admission limit reached", null, provenance));
            }
            var batch = buffers.get(key);
            if (batch == null) {
                batch = new Batch();
                buffers.put(key, batch);
            }
            var wasEmpty = batch.parts.isEmpty();
            batch.parts.add(in);
            batch.items += items;
            batch.bytes += bytes;
            batch.unweighedItems += unweighedItems;
            batch.submissions++;
            // Accepted work must not retain a transport request's cancellation/deadline state. Keep
            // immutable client metadata for routing while giving downstream delivery its own lifetime.
            batch.lastContext = detachedContext;
            if (wasEmpty) {
                scheduleTimeout(batch, key);
            }
            snapshot = shouldFlush(batch) ? removeLocked(key) : null;
            if (snapshot != null) {
                // The flush is performed while holding the same admission lock. Therefore a concurrent
                // flush cannot establish a barrier between removing this snapshot and queue admission.
                enqueue(snapshot);
            }
        }
        return completed(DeliveryResult.accepted(provenance));
    }

    private boolean admit(MetadataBatchKey key, long bytes, long unweighedItems) {
        if (pendingSubmissions.get() >= maxPendingSubmissions) {
            return false;
        }
        if (bytes > maxPendingBytes || pendingBytes.get() > maxPendingBytes - bytes) {
            return false;
        }
        // The item limit concerns only a submission that carries unweighed items. One larger than the
        // whole limit is admitted when nothing unweighed is pending, so it is not refused forever; the
        // pending count can then exceed the limit, which must not block submissions charged in bytes.
        var pendingItems = pendingUnweighedItems.get();
        if (unweighedItems > 0 && pendingItems > 0 && unweighedItems > maxPendingItems - pendingItems) {
            return false;
        }
        if (!buffers.containsKey(key) && buffers.size() >= maxPartitions) {
            return false;
        }
        pendingSubmissions.incrementAndGet();
        pendingBytes.addAndGet(bytes);
        pendingUnweighedItems.addAndGet(unweighedItems);
        return true;
    }

    private void release(long submissions, long bytes, long unweighedItems) {
        pendingSubmissions.addAndGet(-submissions);
        pendingBytes.addAndGet(-bytes);
        pendingUnweighedItems.addAndGet(-unweighedItems);
    }

    private MetadataBatchKey computeKey(ClientMetadata metadata) {
        if (metadataKeys.isEmpty()) {
            return MetadataBatchKey.GLOBAL;
        }
        var headers = metadata.headers();
        var values = new ArrayList<@Nullable List<String>>(metadataKeys.size());
        for (var key : metadataKeys) {
            var value = headers.get(key);
            values.add(value == null ? null : List.copyOf(value));
        }
        return new MetadataBatchKey(values);
    }

    private boolean shouldFlush(Batch batch) {
        return batch.items >= sendBatchSize || sendBatchMaxBytes > 0 && batch.bytes >= sendBatchMaxBytes;
    }

    private void scheduleTimeout(Batch batch, MetadataBatchKey key) {
        var exec = scheduler;
        if (timeoutNanos <= 0 || exec == null) {
            return;
        }
        batch.timer = exec.schedule(() -> onTimeout(key), timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void onTimeout(MetadataBatchKey key) {
        synchronized (lock) {
            var snapshot = removeLocked(key);
            if (snapshot != null && !snapshot.parts.isEmpty()) {
                enqueue(snapshot);
            }
        }
    }

    private @Nullable Batch removeLocked(MetadataBatchKey key) {
        var batch = buffers.remove(key);
        if (batch != null && batch.timer != null) {
            batch.timer.cancel(false);
        }
        return batch;
    }

    private void enqueue(Batch batch) {
        var context = Objects.requireNonNull(batch.lastContext, "buffered context");
        List<Payload<M>> units;
        try {
            var merged = signal.merge(batch.parts);
            units = ExactBatchSplitter.split(signal, merged, sendBatchMaxSize, sendBatchMaxBytes, sizer);
        } catch (Throwable failure) {
            release(batch.submissions, batch.bytes, batch.unweighedItems);
            failures.get().record(batch.items, failure);
            if (failure instanceof Error fatal) {
                throw fatal;
            }
            return;
        }
        var release = new ReleaseOnce(batch.submissions, batch.bytes, batch.unweighedItems);
        var result = queue.offerAll(context, units, release::run);
        if (result.result() instanceof DeliveryResult.Rejected) {
            // Immediate refusal is already included in the queue's flush accumulator. Release the
            // batch accounting because no queue completion callback will run.
            release.run();
        }
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "batch-flush-" + provenance.componentId());
            thread.setDaemon(true);
            return thread;
        });
        queue.start();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<FlushReport> flush(FlushContext context) {
        Objects.requireNonNull(context, "context");
        var deadline = new FlushDeadline(nanoTime.getAsLong(), context.deadline());
        final FailureAccumulator local;
        synchronized (lock) {
            for (var key : new ArrayList<>(buffers.keySet())) {
                var snapshot = removeLocked(key);
                if (snapshot != null && !snapshot.parts.isEmpty()) {
                    enqueue(snapshot);
                }
            }
            // Swap under the same lock used by consume/enqueue. If a concurrent flush times out,
            // its local failures can then be merged into whichever accumulator owns the next barrier
            // without racing a getAndSet and orphaning the late merge.
            local = failures.getAndSet(new FailureAccumulator());
        }
        var drained = queue.flush(deadline.remaining());
        var result = new CompletableFuture<FlushReport>();
        drained.whenComplete((ignored, failure) -> {
            if (failure != null) {
                // A timeout has no report result. Preserve local merge/materialisation failures for
                // the next successful barrier instead of silently losing them with the exceptional stage.
                synchronized (lock) {
                    failures.get().merge(local);
                }
                result.completeExceptionally(failure);
            } else {
                try {
                    result.complete(local.report().plus(ignored));
                } catch (Throwable mergeFailure) {
                    result.completeExceptionally(mergeFailure);
                }
            }
        });
        return result;
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        Objects.requireNonNull(context, "context");
        var existing = shutdownResult.get();
        if (existing != null) {
            return existing;
        }
        var terminal = new CompletableFuture<Void>();
        if (!shutdownResult.compareAndSet(null, terminal)) {
            return Objects.requireNonNull(shutdownResult.get());
        }

        try {
            var deadline = new FlushDeadline(nanoTime.getAsLong(), context.deadline());
            final FailureAccumulator local;
            synchronized (lock) {
                closed = true;
                for (var key : new ArrayList<>(buffers.keySet())) {
                    var snapshot = removeLocked(key);
                    if (snapshot != null && !snapshot.parts.isEmpty()) {
                        enqueue(snapshot);
                    }
                }
                local = failures.getAndSet(new FailureAccumulator());
            }
            var drained = queue.drainAndStop(deadline.remaining());
            drained.whenComplete((report, failure) -> {
                if (failure != null) {
                    synchronized (lock) {
                        failures.get().merge(local);
                    }
                    terminal.completeExceptionally(failure);
                    return;
                }
                try {
                    var combined = local.report().plus(report);
                    if (combined.isSuccess()) {
                        terminal.complete(null);
                    } else {
                        var firstFailure = combined.firstFailure();
                        terminal.completeExceptionally(firstFailure != null
                                ? firstFailure
                                : new IllegalStateException("batch processor shutdown lost telemetry"));
                    }
                } catch (Throwable mergeFailure) {
                    terminal.completeExceptionally(mergeFailure);
                }
            });
            var exec = scheduler;
            if (exec != null) {
                exec.shutdownNow();
            }
        } catch (Throwable failure) {
            terminal.completeExceptionally(failure);
        }
        return terminal;
    }

    private static <T> CompletionStage<T> completed(T result) {
        return CompletableFuture.completedFuture(result);
    }

    private final class ReleaseOnce {
        private final long submissions;
        private final long bytes;
        private final long unweighedItems;
        private final AtomicBoolean released = new AtomicBoolean();

        private ReleaseOnce(long submissions, long bytes, long unweighedItems) {
            this.submissions = submissions;
            this.bytes = bytes;
            this.unweighedItems = unweighedItems;
        }

        void run() {
            if (released.compareAndSet(false, true)) {
                release(submissions, bytes, unweighedItems);
            }
        }
    }

    private final class Batch {
        private final List<M> parts = new ArrayList<>();
        private long items;
        private long bytes;
        private long unweighedItems;
        private long submissions;
        private @Nullable ConsumeContext lastContext;
        private @Nullable ScheduledFuture<?> timer;
    }

    private static final class MetadataBatchKey {
        private static final MetadataBatchKey GLOBAL = new MetadataBatchKey(List.of());
        private final List<@Nullable List<String>> values;

        private MetadataBatchKey(List<@Nullable List<String>> values) {
            // List.copyOf rejects null elements, but null distinguishes an unset metadata key from
            // an explicitly empty header list and is part of the partition key.
            this.values = Collections.unmodifiableList(new ArrayList<>(values));
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof MetadataBatchKey key && values.equals(key.values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }

    private final class FailureAccumulator {
        private long rejected;
        private long failed;
        private @Nullable Throwable first;

        synchronized void record(long items, Throwable failure) {
            rejected += items;
            failed++;
            if (first == null) {
                first = failure;
            }
        }

        synchronized void merge(FailureAccumulator other) {
            rejected += other.rejected;
            failed += other.failed;
            if (first == null) {
                first = other.first;
            }
        }

        synchronized FlushReport report() {
            return new FlushReport(rejected, 0, failed, first);
        }
    }

    private final class FlushDeadline {
        private long end;

        private FlushDeadline(long start, Duration duration) {
            long nanos;
            try {
                nanos = duration.toNanos();
                end = Math.addExact(start, nanos);
            } catch (ArithmeticException e) {
                end = Long.MAX_VALUE;
            }
        }

        Duration remaining() {
            return Duration.ofNanos(Math.max(0, end - nanoTime.getAsLong()));
        }
    }
}
