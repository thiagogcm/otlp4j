package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.receiver.TapOptions;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;

/// Internal multicast [Flow.Publisher].
///
/// Each subscription owns a bounded queue and a virtual-thread dispatcher. A slow
/// subscriber drains its own queue under [TapOptions] without back-pressuring the producer.
final class MulticastPublisher<T> implements Flow.Publisher<T>, AutoCloseable {

    private static final Flow.Subscription NO_OP_SUBSCRIPTION = new Flow.Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
    };

    private final CopyOnWriteArrayList<SubscriptionImpl<T>> subscriptions = new CopyOnWriteArrayList<>();
    private final LongAdder drops;
    private volatile boolean closed;

    public MulticastPublisher(LongAdder drops) {
        this.drops = Objects.requireNonNull(drops, "drops");
    }

    public void publish(T item) {
        if (closed || subscriptions.isEmpty()) {
            return;
        }
        for (var sub : subscriptions) {
            sub.offer(item);
        }
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        subscribe(subscriber, TapOptions.defaults());
    }

    /// Subscribes with `options` bound to this subscription's buffer and overflow policy, so
    /// configuration travels with the subscription instead of shared mutable tap state.
    public void subscribe(Flow.Subscriber<? super T> subscriber, TapOptions options) {
        Objects.requireNonNull(subscriber, "subscriber");
        Objects.requireNonNull(options, "options");
        if (closed) {
            subscriber.onSubscribe(NO_OP_SUBSCRIPTION);
            subscriber.onComplete();
            return;
        }
        var sub = new SubscriptionImpl<>(subscriber, options, drops, subscriptions);
        subscriptions.add(sub);
        subscriber.onSubscribe(sub);
        sub.startDispatcher();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (var sub : subscriptions) {
            sub.complete();
        }
        subscriptions.clear();
    }

    private static final class SubscriptionImpl<T> implements Flow.Subscription {

        private final Flow.Subscriber<? super T> subscriber;
        private final TapOptions.Overflow strategy;
        private final ArrayBlockingQueue<T> queue;
        private final LongAdder drops;
        private final CopyOnWriteArrayList<SubscriptionImpl<T>> roster;
        private final Semaphore demand = new Semaphore(0);
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean dispatcherRunning = new AtomicBoolean();
        private volatile @Nullable Thread dispatcher;

        SubscriptionImpl(
                Flow.Subscriber<? super T> subscriber,
                TapOptions options,
                LongAdder drops,
                CopyOnWriteArrayList<SubscriptionImpl<T>> roster) {
            this.subscriber = subscriber;
            this.strategy = options.strategy();
            this.queue = new ArrayBlockingQueue<>(options.bufferSize());
            this.drops = drops;
            this.roster = roster;
        }

        /// Never blocks: the tap sits outside the acknowledgement path, so every overflow
        /// strategy resolves immediately on the publishing thread.
        void offer(T item) {
            if (cancelled.get()) {
                return;
            }
            switch (strategy) {
                case DROP_OLDEST -> {
                    if (!queue.offer(item)) {
                        if (queue.poll() != null) {
                            drops.increment();
                        }
                        queue.offer(item);
                    }
                }
                case DROP_NEWEST -> {
                    if (!queue.offer(item)) {
                        drops.increment();
                    }
                }
                case FAIL -> {
                    if (!queue.offer(item)) {
                        drops.increment();
                        completeExceptionally(new IllegalStateException("tap buffer overflow"));
                    }
                }
            }
        }

        void startDispatcher() {
            if (dispatcherRunning.compareAndSet(false, true)) {
                // Publish the field before start() so complete()/cancel() can interrupt the dispatcher.
                var t = Thread.ofVirtual().name("otlp4j-tap-dispatcher").unstarted(this::dispatchLoop);
                dispatcher = t;
                t.start();
            }
        }

        /// Blocks for demand, then an item, then delivers (a `Flow.Publisher` must not exceed
        /// demand).
        private void dispatchLoop() {
            try {
                while (!cancelled.get()) {
                    demand.acquire();
                    if (cancelled.get()) {
                        return;
                    }
                    var item = queue.take();
                    if (cancelled.get()) {
                        return;
                    }
                    try {
                        subscriber.onNext(item);
                    } catch (RuntimeException e) {
                        completeExceptionally(e);
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        void complete() {
            if (cancelled.compareAndSet(false, true)) {
                terminateDispatch();
                try {
                    subscriber.onComplete();
                } catch (RuntimeException _) {
                    // Subscriber violated Flow contract; cannot propagate further.
                }
            }
        }

        void completeExceptionally(Throwable t) {
            if (cancelled.compareAndSet(false, true)) {
                terminateDispatch();
                try {
                    subscriber.onError(t);
                } catch (RuntimeException _) {
                    // Subscriber violated Flow contract; cannot propagate further.
                }
                roster.remove(this);
            }
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                completeExceptionally(new IllegalArgumentException("request must be positive"));
                return;
            }
            // Saturate at Integer.MAX_VALUE outstanding permits; effectively unbounded demand.
            var headroom = Integer.MAX_VALUE - demand.availablePermits();
            if (headroom > 0) {
                demand.release((int) Math.min(n, headroom));
            }
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                roster.remove(this);
                terminateDispatch();
            }
        }

        /// Interrupts the dispatcher (releasing its blocking `take`) and drains the queue.
        private void terminateDispatch() {
            var d = dispatcher;
            if (d != null) {
                d.interrupt();
            }
            queue.clear();
        }
    }
}
