package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.receiver.TapOptions;
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
        try {
            pub.subscribe(FlowSubscribers.noOp(), new TapOptions(TapOptions.Overflow.DROP_OLDEST, 2));
            for (var i = 0; i < 10; i++) {
                pub.publish(i);
            }
            assertThat(drops.sum()).isGreaterThan(0L);
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
            }, new TapOptions(TapOptions.Overflow.FAIL, 1));
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
            }, new TapOptions(TapOptions.Overflow.DROP_OLDEST, 16));
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
