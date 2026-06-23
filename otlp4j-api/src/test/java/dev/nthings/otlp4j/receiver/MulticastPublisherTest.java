package dev.nthings.otlp4j.receiver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.testing.FlowSubscribers;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("MulticastPublisher")
class MulticastPublisherTest {

    @DisplayName("Published items reach a subscribed Flow.Subscriber")
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

    @DisplayName("DROP_NEWEST drops items instead of blocking the producer")
    @Test
    void droppingSubscriberDoesNotBlockProducer() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<Integer>(drops);
        pub.setOptions(new TapOptions(BackpressureStrategy.DROP_NEWEST, 2));
        pub.subscribe(FlowSubscribers.noOp());
        for (var i = 0; i < 50; i++) {
            pub.publish(i);
        }
        assertThat(drops.sum()).isGreaterThan(0);
        pub.close();
    }

    @DisplayName("Buffered item is delivered once demand is requested")
    @Test
    void deliversBufferedItemPromptlyOnceDemandArrives() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        var received = new CopyOnWriteArrayList<String>();
        var subRef = new AtomicReference<Flow.Subscription>();
        pub.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription s) { subRef.set(s); } // no demand yet
            @Override public void onNext(String item) { received.add(item); }
            @Override public void onError(Throwable t) {}
            @Override public void onComplete() {}
        });

        pub.publish("buffered");
        assertThat(received).as("nothing is delivered before demand is requested").isEmpty();

        subRef.get().request(1);
        await().atMost(Duration.ofSeconds(1)).until(() -> received.size() == 1);
        assertThat(received).containsExactly("buffered");
        pub.close();
    }

    @DisplayName("close() signals onComplete to every subscriber")
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
