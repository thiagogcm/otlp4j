package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.testing.FlowSubscribers;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("MulticastPublisher")
class MulticastPublisherTest {

    @Test
    void closeDuringSubscriptionDeliversCompletionAfterOnSubscribeReturns() throws Exception {
        try (var publisher = new MulticastPublisher<String>(new LongAdder())) {
            var entered = new CompletableFuture<Void>();
            var release = new CompletableFuture<Void>();
            var subscribed = new CompletableFuture<Void>();
            var events = new CopyOnWriteArrayList<String>();
            Thread.ofVirtual().start(() -> {
                publisher.subscribe(new Flow.Subscriber<>() {
                    @Override public void onSubscribe(Flow.Subscription subscription) {
                        entered.complete(null);
                        release.join();
                        events.add("subscribed");
                        subscription.request(Long.MAX_VALUE);
                    }
                    @Override public void onNext(String item) { events.add(item); }
                    @Override public void onError(Throwable failure) { events.add("error"); }
                    @Override public void onComplete() { events.add("complete"); }
                });
                subscribed.complete(null);
            });
            try {
                entered.get(2, TimeUnit.SECONDS);
                publisher.close();
                assertThat(events).isEmpty();
            } finally { release.complete(null); }
            subscribed.get(2, TimeUnit.SECONDS);
            assertThat(events).containsExactly("subscribed", "complete");
        }
    }

    @Test
    void cancellingInsideOnSubscribeNeverRegistersAStalledSubscriber() {
        var events = new CopyOnWriteArrayList<String>();
        try (var publisher = new MulticastPublisher<String>(new LongAdder())) {
            publisher.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription subscription) { subscription.cancel(); }
                @Override public void onNext(String item) { events.add(item); }
                @Override public void onError(Throwable failure) { events.add("error"); }
                @Override public void onComplete() { events.add("complete"); }
            });
            publisher.publish("ignored");
        }
        assertThat(events).isEmpty();
    }

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

    @DisplayName("close() is idempotent and publishing afterwards is a silent no-op")
    @Test
    void closeIsIdempotentAndDropsLaterPublishes() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        pub.subscribe(FlowSubscribers.noOp());
        pub.close();
        pub.close(); // second close is a no-op
        pub.publish("after-close"); // publishing on a closed publisher is dropped
        assertThat(drops.sum()).isZero();
    }

    @DisplayName("publish with no subscribers is a no-op")
    @Test
    void publishWithoutSubscribersIsNoOp() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        pub.publish("ignored"); // no subscribers attached
        assertThat(drops.sum()).isZero();
        pub.close();
    }
}
