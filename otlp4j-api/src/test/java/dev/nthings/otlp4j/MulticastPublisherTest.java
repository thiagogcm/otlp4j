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
import org.junit.jupiter.api.Test;

class MulticastPublisherTest {

    @Test
    void deliversToSubscriber() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        var received = new CopyOnWriteArrayList<String>();
        pub.subscribe(FlowSubscribers.recording(received, 100));

        pub.publish("a");
        pub.publish("b");
        await().atMost(Duration.ofSeconds(2)).until(() -> received.size() >= 2);
        assertThat(received).containsExactly("a", "b");
        pub.close();
    }

    @Test
    void droppingSubscriberDoesNotBlockProducer() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        pub.setOptions(new TapOptions(BackpressureStrategy.DROP_NEWEST, 2));
        pub.subscribe(FlowSubscribers.noOp());
        for (int i = 0; i < 50; i++) {
            pub.publish(i);
        }
        assertThat(drops.sum()).isGreaterThan(0);
        pub.close();
    }

    @Test
    void closeCompletesEverySubscriber() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        var done = new AtomicReference<>(false);
        pub.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onNext(String item) {}
            @Override public void onError(Throwable t) {}
            @Override public void onComplete() { done.set(true); }
        });
        pub.close();
        await().atMost(Duration.ofSeconds(2)).until(done::get);
        assertThat(done.get()).isTrue();
    }

}
