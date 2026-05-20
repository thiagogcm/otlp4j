package dev.nthings.otlp4j.receiver.internal;

import dev.nthings.otlp4j.receiver.BackpressureStrategy;
import dev.nthings.otlp4j.receiver.TapOptions;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Internal multicast [Flow.Publisher].
///
/// Each subscription owns a bounded queue and a virtual-thread dispatcher; a slow subscriber
/// drains its own queue under [TapOptions] without back-pressuring the producer.
public final class MulticastPublisher<T> implements Flow.Publisher<T>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MulticastPublisher.class);

    private static final Flow.Subscription NO_OP_SUBSCRIPTION = new Flow.Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
    };

    private final CopyOnWriteArrayList<SubscriptionImpl<T>> subscriptions = new CopyOnWriteArrayList<>();
    private final LongAdder drops;
    private volatile TapOptions options = TapOptions.defaults();
    private volatile boolean closed;

    public MulticastPublisher(LongAdder drops) {
        this.drops = Objects.requireNonNull(drops, "drops");
    }

    public void setOptions(TapOptions options) {
        this.options = Objects.requireNonNull(options, "options");
    }

    /// True when at least one subscriber is currently attached.
    public boolean hasSubscribers() {
        return !subscriptions.isEmpty();
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
        Objects.requireNonNull(subscriber, "subscriber");
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
        private final BackpressureStrategy strategy;
        private final ArrayBlockingQueue<T> queue;
        private final LongAdder drops;
        private final CopyOnWriteArrayList<SubscriptionImpl<T>> roster;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean dispatcherRunning = new AtomicBoolean();
        private Thread dispatcher;

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
                case BLOCK -> {
                    try {
                        queue.put(item);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                case ERROR -> {
                    if (!queue.offer(item)) {
                        drops.increment();
                        completeExceptionally(new IllegalStateException("tap buffer overflow"));
                    }
                }
            }
        }

        void startDispatcher() {
            if (dispatcherRunning.compareAndSet(false, true)) {
                dispatcher = Thread.ofVirtual()
                        .name("otlp4j-tap-dispatcher")
                        .start(this::dispatchLoop);
            }
        }

        private void dispatchLoop() {
            try {
                while (!cancelled.get()) {
                    while (demand.get() > 0 || strategy == BackpressureStrategy.BLOCK) {
                        T item = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (item == null) {
                            if (cancelled.get()) return;
                            continue;
                        }
                        if (cancelled.get()) return;
                        if (demand.get() > 0) {
                            demand.decrementAndGet();
                        }
                        try {
                            subscriber.onNext(item);
                        } catch (RuntimeException e) {
                            completeExceptionally(e);
                            return;
                        }
                    }
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        void complete() {
            if (cancelled.compareAndSet(false, true)) {
                try {
                    subscriber.onComplete();
                } catch (RuntimeException ignored) {
                    // Subscriber violated Flow contract; cannot propagate further.
                }
            }
        }

        void completeExceptionally(Throwable t) {
            if (cancelled.compareAndSet(false, true)) {
                try {
                    subscriber.onError(t);
                } catch (RuntimeException ignored) {
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
            // Saturating add: concurrent request() calls must never overflow demand.
            demand.updateAndGet(prev -> prev > Long.MAX_VALUE - n ? Long.MAX_VALUE : prev + n);
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                roster.remove(this);
                if (dispatcher != null) {
                    dispatcher.interrupt();
                }
            }
        }
    }
}
