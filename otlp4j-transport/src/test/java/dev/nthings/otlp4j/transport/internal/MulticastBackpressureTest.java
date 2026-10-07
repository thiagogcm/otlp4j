package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@DisplayName("MulticastPublisher backpressure")
class MulticastBackpressureTest {

    @Test
    @DisplayName("concurrent producers account for every item evicted from a stalled tap")
    void concurrentOverflowAccountsForEveryLostItem() throws Exception {
        var drops = new LongAdder();
        var received = new CopyOnWriteArrayList<Integer>();
        var subscription = new AtomicReference<Flow.Subscription>();
        var start = new CountDownLatch(1);
        var producers = 8;
        var itemsPerProducer = 1_000;
        try (var publisher = new MulticastPublisher<Integer>(drops);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            publisher.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { subscription.set(s); }
                @Override public void onNext(Integer item) { received.add(item); }
                @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
                @Override public void onComplete() {}
            }, new TapOptions(OverflowPolicy.DROP_OLDEST, 1));
            var tasks = new ArrayList<Future<?>>();
            for (var producer = 0; producer < producers; producer++) {
                var offset = producer * itemsPerProducer;
                tasks.add(executor.submit(() -> {
                    start.await();
                    for (var item = 0; item < itemsPerProducer; item++) {
                        publisher.publish(offset + item);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var task : tasks) {
                task.get();
            }
            subscription.get().request(1);
            await().atMost(Duration.ofSeconds(2)).until(() -> received.size() == 1);
            assertThat(drops.sum() + received.size()).isEqualTo(producers * itemsPerProducer);
        }
    }

    @DisplayName("overflow retains the correct buffered items and counts every eviction")
    @ParameterizedTest
    @EnumSource(value = OverflowPolicy.class, names = {"DROP_OLDEST", "DROP_NEWEST"})
    void overflowRetainsTheCorrectItems(OverflowPolicy policy) {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        try {
            var sink = new CopyOnWriteArrayList<Integer>();
            var subscription = new AtomicReference<Flow.Subscription>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { subscription.set(s); }
                @Override public void onNext(Integer item) { sink.add(item); }
                @Override public void onError(Throwable t) { throw new AssertionError(t); }
                @Override public void onComplete() {}
            }, new TapOptions(policy, 2));
            for (var i = 0; i < 5; i++) {
                pub.publish(i);
            }
            assertThat(drops.sum()).isEqualTo(3L);
            subscription.get().request(2);
            await().atMost(Duration.ofSeconds(2)).until(() -> sink.size() == 2);
            if (policy == OverflowPolicy.DROP_OLDEST) {
                assertThat(sink).containsExactly(3, 4);
            } else {
                assertThat(sink).containsExactly(0, 1);
            }
        } finally {
            pub.close();
        }
    }

    @DisplayName("FAIL strategy signals overflow via onError")
    @Test
    void errorStrategySignalsViaOnError() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        try {
            var error = new AtomicReference<Throwable>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) {}
                @Override public void onNext(Integer item) {}
                @Override public void onError(Throwable t) { error.set(t); }
                @Override public void onComplete() {}
            }, new TapOptions(OverflowPolicy.FAIL, 1));
            for (var i = 0; i < 10; i++) {
                pub.publish(i);
            }
            await().atMost(Duration.ofSeconds(2)).until(() -> error.get() != null);
            assertThat(error.get()).isInstanceOf(IllegalStateException.class);
        } finally {
            pub.close();
        }
    }

    @DisplayName("Dispatch honours bounded demand and never delivers more than requested")
    @Test
    void honoursBoundedDemand() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        try {
            var sink = new CopyOnWriteArrayList<Integer>();
            var subRef = new AtomicReference<Flow.Subscription>();
            // Buffer > publish count so only demand can gate delivery.
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { subRef.set(s); s.request(2); }
                @Override public void onNext(Integer item) { sink.add(item); }
                @Override public void onError(Throwable t) {}
                @Override public void onComplete() {}
            }, new TapOptions(OverflowPolicy.DROP_OLDEST, 16));
            for (var i = 0; i < 5; i++) {
                pub.publish(i);
            }

            // Only the 2 requested are delivered; the rest stay buffered even after the dispatcher
            // has had time to over-deliver.
            await().atMost(Duration.ofSeconds(2)).until(() -> sink.size() == 2);
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
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
