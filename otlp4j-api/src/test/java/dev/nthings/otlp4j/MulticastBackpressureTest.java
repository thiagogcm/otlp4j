package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.receiver.BackpressureStrategy;
import dev.nthings.otlp4j.receiver.TapOptions;
import dev.nthings.otlp4j.receiver.internal.MulticastPublisher;
import dev.nthings.otlp4j.testing.FlowSubscribers;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
            for (int i = 0; i < 10; i++) {
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
            for (int i = 0; i < 10; i++) {
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
            for (int i = 0; i < 6; i++) {
                pub.publish(i);
            }
            await().atMost(Duration.ofSeconds(3)).until(() -> sink.size() == 6);
            assertThat(sink).hasSize(6);
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
