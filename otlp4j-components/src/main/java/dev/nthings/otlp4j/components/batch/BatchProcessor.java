package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.components.internal.DeliveryQueue;
import dev.nthings.otlp4j.components.internal.Sizer;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The built-in batch processor. It buffers incoming payloads, partitioned by the metadata-key
/// combination read from the consume context, and flushes each buffer when its accumulated item count
/// reaches the target size, its accumulated byte size reaches the byte cap, or the flush timeout
/// elapses since the buffer's first item. On flush it merges the buffered domain batches through the
/// signal's `merge` and, when a send-batch maximum size is set, splits the merged batch into
/// item-capped units through the signal's `split`, so the batching logic is entirely generic over the
/// signal with no per-signal switch. Each unit is handed to the reusable bounded [DeliveryQueue].
///
/// The processor is non-mutating: it emits fresh merged and split batches and never mutates an input.
///
/// @param <M> the signal's domain batch type this processor buffers, merges, and splits
public final class BatchProcessor<M> implements Processor<TelemetryPayload> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchProcessor.class);

    private final SignalDescriptor<M> signal;
    private final int sendBatchSize;
    private final int sendBatchMaxSize;
    private final long sendBatchMaxBytes;
    private final long timeoutNanos;
    private final List<String> metadataKeys;
    private final SignalConsumer<TelemetryPayload> next;
    private final Provenance provenance;
    private final DeliveryQueue<TelemetryPayload> queue;

    private final Object lock = new Object();
    private final Map<MetadataBatchKey, Batch> buffers = new HashMap<>();
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();

    private volatile boolean closed;
    private volatile @Nullable ScheduledExecutorService scheduler;

    /// Builds a batch processor for `signal` forwarding to `next`, honouring `config`.
    ///
    /// @param id       the component identity
    /// @param signal   the signal this processor buffers, merges, and splits
    /// @param config   the batch configuration
    /// @param next     the downstream consumer the delivery queue drains into
    /// @param nanoTime the monotonic nanosecond source the delivery queue's degraded window reads
    BatchProcessor(
            ComponentId id,
            SignalDescriptor<M> signal,
            BatchProcessorConfig config,
            SignalConsumer<TelemetryPayload> next,
            LongSupplier nanoTime) {
        this.signal = signal;
        this.sendBatchSize = config.batch().sendBatchSize();
        this.sendBatchMaxSize = config.batch().sendBatchMaxSize();
        this.sendBatchMaxBytes = config.batch().sendBatchMaxBytes();
        this.timeoutNanos = config.batch().timeout().toNanos();
        this.metadataKeys = config.batch().metadataKeys();
        this.next = next;
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.queue = new DeliveryQueue<>(config.delivery(), config.degradedWindow(), next, provenance, nanoTime);
    }

    @Override
    public ConsumerCapabilities capabilities() {
        return ConsumerCapabilities.nonMutating();
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
        if (closed) {
            return CompletableFuture.completedFuture(
                    DeliveryResult.shuttingDown("batch processor is shutting down", provenance));
        }
        var in = signal.batchType().cast(data.view(PayloadView.DOMAIN_IMMUTABLE));
        var items = data.itemCount();
        var bytes = data.estimatedSerializedSize();
        var key = computeKey(context);
        Batch snapshot;
        synchronized (lock) {
            var batch = buffers.computeIfAbsent(key, k -> new Batch());
            var wasEmpty = batch.parts.isEmpty();
            batch.parts.add(in);
            batch.items += items;
            batch.bytes += bytes;
            batch.lastContext = context;
            if (wasEmpty) {
                scheduleTimeout(batch, key);
            }
            snapshot = shouldFlush(batch) ? removeLocked(key) : null;
        }
        if (snapshot == null) {
            return DeliveryResult.acceptedStage(provenance);
        }
        return combine(enqueue(context, snapshot.parts));
    }

    private MetadataBatchKey computeKey(ConsumeContext context) {
        if (metadataKeys.isEmpty()) {
            return MetadataBatchKey.GLOBAL;
        }
        var headers = context.metadata().headers();
        var values = new ArrayList<@Nullable List<String>>(metadataKeys.size());
        for (var key : metadataKeys) {
            // A key absent from the headers stores null, distinct from a present-but-empty value list,
            // so unset and empty metadata are distinct batch partitions, matching the Collector.
            values.add(headers.get(key));
        }
        return new MetadataBatchKey(values);
    }

    private boolean shouldFlush(Batch batch) {
        if (Sizer.ITEMS.read(batch.items, batch.bytes) >= sendBatchSize) {
            return true;
        }
        return sendBatchMaxBytes > 0 && Sizer.BYTES.read(batch.items, batch.bytes) >= sendBatchMaxBytes;
    }

    private void scheduleTimeout(Batch batch, MetadataBatchKey key) {
        var exec = scheduler;
        if (timeoutNanos <= 0 || exec == null) {
            return;
        }
        batch.timer = exec.schedule(() -> onTimeout(key), timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void onTimeout(MetadataBatchKey key) {
        Batch snapshot;
        synchronized (lock) {
            snapshot = removeLocked(key);
        }
        if (snapshot == null || snapshot.parts.isEmpty()) {
            return;
        }
        // A timeout flush is not returned to any caller, so failures are logged, not signalled upstream.
        var context = Objects.requireNonNull(snapshot.lastContext, "buffered context");
        enqueue(context, snapshot.parts).forEach(stage -> stage.whenComplete(this::logFlushOutcome));
    }

    /// Removes and returns the batch under `key`, cancelling its pending timeout. Returns null when no
    /// batch is buffered under the key (a concurrent flush already claimed it). Must hold the lock.
    private @Nullable Batch removeLocked(MetadataBatchKey key) {
        var batch = buffers.remove(key);
        if (batch != null && batch.timer != null) {
            batch.timer.cancel(false);
        }
        return batch;
    }

    /// Merges the buffered parts, splits the merged batch when a send-batch maximum size is set, and
    /// enqueues each unit into the delivery queue, returning the per-unit delivery stages in order.
    private List<CompletionStage<DeliveryResult>> enqueue(ConsumeContext context, List<M> parts) {
        var merged = signal.merge(parts);
        var units = sendBatchMaxSize > 0 ? signal.split(merged, sendBatchMaxSize) : List.of(merged);
        var stages = new ArrayList<CompletionStage<DeliveryResult>>(units.size());
        for (var unit : units) {
            stages.add(queue.offer(context, Payloads.domain(signal, unit)));
        }
        return stages;
    }

    /// Combines the per-unit delivery stages of one flush into a single result: the first rejection
    /// wins, otherwise the first partial, otherwise acceptance.
    private CompletionStage<DeliveryResult> combine(List<CompletionStage<DeliveryResult>> stages) {
        if (stages.size() == 1) {
            return stages.get(0);
        }
        var futures = stages.stream().map(CompletionStage::toCompletableFuture).toList();
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> reduce(futures));
    }

    private DeliveryResult reduce(List<CompletableFuture<DeliveryResult>> futures) {
        DeliveryResult firstPartial = null;
        for (var future : futures) {
            var result = future.join();
            if (result instanceof DeliveryResult.Rejected) {
                return result;
            }
            if (result instanceof DeliveryResult.Partial && firstPartial == null) {
                firstPartial = result;
            }
        }
        return firstPartial != null ? firstPartial : DeliveryResult.accepted(provenance);
    }

    private void logFlushOutcome(@Nullable DeliveryResult result, @Nullable Throwable failure) {
        if (failure != null) {
            LOG.warn("batch flush failed for {}", provenance.componentId(), failure);
        } else if (result instanceof DeliveryResult.Rejected rejected) {
            LOG.warn("batch flush rejected downstream for {}: {}",
                    provenance.componentId(), rejected.message());
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
    public CompletionStage<Void> forceFlush(FlushContext context) {
        flushAll();
        queue.awaitDrained(context.deadline());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        flushAll();
        queue.drainAndStop(context.deadline());
        var exec = scheduler;
        if (exec != null) {
            exec.shutdownNow();
        }
        return CompletableFuture.completedFuture(null);
    }

    /// Flushes every buffered partition, enqueuing each into the delivery queue and logging any
    /// per-flush failure, since a flush at flush-all time is not returned to any caller.
    private void flushAll() {
        List<Batch> snapshots = new ArrayList<>();
        synchronized (lock) {
            for (var key : new ArrayList<>(buffers.keySet())) {
                snapshots.add(removeLocked(key));
            }
        }
        for (var batch : snapshots) {
            if (batch == null || batch.parts.isEmpty()) {
                continue;
            }
            var context = Objects.requireNonNull(batch.lastContext, "buffered context");
            enqueue(context, batch.parts).forEach(stage -> stage.whenComplete(this::logFlushOutcome));
        }
    }

    /// One metadata-keyed accumulator: the buffered domain batches, the running item and byte totals,
    /// the most recent delivery context (used for a timeout or flush-all flush), and the pending
    /// timeout task. A non-static inner class so its buffered parts are typed to the enclosing
    /// processor's signal batch type.
    private final class Batch {
        private final List<M> parts = new ArrayList<>();
        private long items;
        private long bytes;
        private @Nullable ConsumeContext lastContext;
        private @Nullable ScheduledFuture<?> timer;
    }

    /// A batch partition key: the ordered list of the configured metadata keys' header values, with a
    /// null element for an unset key so it is distinct from a present-but-empty value list. The empty
    /// key set collapses to the shared [#GLOBAL] partition.
    private static final class MetadataBatchKey {

        private static final MetadataBatchKey GLOBAL = new MetadataBatchKey(List.of());

        private final List<@Nullable List<String>> values;

        private MetadataBatchKey(List<@Nullable List<String>> values) {
            this.values = values;
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
}
