package dev.nthings.otlp4j.receiver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.testing.FlowSubscribers;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("MulticastPublisher backpressure")
class MulticastBackpressureTest {

    @DisplayName("DROP_OLDEST evicts older items and counts drops")
    @Test
    void dropOldestEvictsOlderItems() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        pub.setOptions(new TapOptions(BackpressureStrategy.DROP_OLDEST, 2));
        try {
            pub.subscribe(FlowSubscribers.noOp());
            for (var i = 0; i < 10; i++) {
                pub.publish(i);
            }
            assertThat(drops.sum()).isGreaterThan(0L);
        } finally {
            pub.close();
        }
    }

    @DisplayName("ERROR strategy signals overflow via onError")
    @Test
    void errorStrategySignalsViaOnError() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        pub.setOptions(new TapOptions(BackpressureStrategy.ERROR, 1));
        try {
            var error = new AtomicReference<Throwable>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) {}
                @Override public void onNext(Integer item) {}
                @Override public void onError(Throwable t) { error.set(t); }
                @Override public void onComplete() {}
            });
            for (var i = 0; i < 10; i++) {
                pub.publish(i);
            }
            await().atMost(Duration.ofSeconds(2)).until(() -> error.get() != null);
            assertThat(error.get()).isInstanceOf(IllegalStateException.class);
        } finally {
            pub.close();
        }
    }

    @DisplayName("BLOCK strategy delivers every item without dropping")
    @Test
    void blockStrategyEventuallyDelivers() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        pub.setOptions(new TapOptions(BackpressureStrategy.BLOCK, 2));
        try {
            var sink = new CopyOnWriteArrayList<Integer>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
                @Override public void onNext(Integer item) {
                    try {
                        // Synchronous slow consumer — BLOCK lets publish() wait for capacity.
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    sink.add(item);
                }
                @Override public void onError(Throwable t) {}
                @Override public void onComplete() {}
            });
            for (var i = 0; i < 6; i++) {
                pub.publish(i);
            }
            await().atMost(Duration.ofSeconds(3)).until(() -> sink.size() == 6);
            assertThat(sink).hasSize(6);
            assertThat(drops.sum()).isZero();
        } finally {
            pub.close();
        }
    }

    @DisplayName("BLOCK honours bounded demand and never delivers more than requested")
    @Test
    void blockHonoursBoundedDemand() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        // Buffer > publish count so only demand can gate delivery; the old BLOCK bypass delivered all.
        pub.setOptions(new TapOptions(BackpressureStrategy.BLOCK, 16));
        try {
            var sink = new CopyOnWriteArrayList<Integer>();
            var subRef = new AtomicReference<Flow.Subscription>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { subRef.set(s); s.request(2); }
                @Override public void onNext(Integer item) { sink.add(item); }
                @Override public void onError(Throwable t) {}
                @Override public void onComplete() {}
            });
            for (var i = 0; i < 5; i++) {
                pub.publish(i);
            }

            // Only the 2 requested are delivered; the rest stay buffered even after the dispatcher
            // has had time to over-deliver.
            await().atMost(Duration.ofSeconds(2)).until(() -> sink.size() == 2);
            await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
                    .until(() -> sink.size() == 2);
            assertThat(sink).containsExactly(0, 1);

            // More demand releases the rest, in order.
            subRef.get().request(10);
            await().atMost(Duration.ofSeconds(2)).until(() -> sink.size() == 5);
            assertThat(sink).containsExactly(0, 1, 2, 3, 4);
            assertThat(drops.sum()).isZero();
        } finally {
            pub.close();
        }
    }

    @DisplayName("Cancelling a BLOCK subscription releases a producer blocked on a full queue")
    @Test
    @Timeout(15)
    void cancelReleasesBlockedBlockProducer() throws InterruptedException {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        pub.setOptions(new TapOptions(BackpressureStrategy.BLOCK, 1));
        var subRef = new AtomicReference<Flow.Subscription>();
        // Subscriber never requests, so the demand-gated dispatcher never drains the queue.
        pub.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription s) { subRef.set(s); }
            @Override public void onNext(Integer item) {}
            @Override public void onError(Throwable t) {}
            @Override public void onComplete() {}
        });
        await().atMost(Duration.ofSeconds(2)).until(() -> subRef.get() != null);

        var producerReturned = new AtomicBoolean();
        var producer = new Thread(() -> {
            // The capacity-1 queue fills on the first item; the next publish blocks in offer().
            for (var i = 0; i < 5; i++) {
                pub.publish(i);
            }
            producerReturned.set(true);
        }, "test-block-producer");
        try {
            producer.start();

            // Nobody draining: the producer must be parked in publish(), not finished.
            Thread.sleep(300);
            assertThat(producerReturned.get()).isFalse();

            // Cancellation must release the parked producer, not hang it.
            subRef.get().cancel();
            await().atMost(Duration.ofSeconds(5)).untilTrue(producerReturned);
        } finally {
            producer.join(Duration.ofSeconds(5));
            pub.close();
        }
        assertThat(producerReturned.get()).isTrue();
    }

    @DisplayName("Subscriber throwing in onNext is signalled via onError")
    @Test
    void subscriberThrowingOnNextCompletesExceptionally() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        try {
            var err = new AtomicReference<Throwable>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
                @Override public void onNext(Integer item) {
                    throw new IllegalStateException("boom");
                }
                @Override public void onError(Throwable t) { err.set(t); }
                @Override public void onComplete() {}
            });
            pub.publish(1);
            await().atMost(Duration.ofSeconds(2)).until(() -> err.get() != null);
            assertThat(err.get()).isInstanceOf(IllegalStateException.class);
        } finally {
            pub.close();
        }
    }

}
