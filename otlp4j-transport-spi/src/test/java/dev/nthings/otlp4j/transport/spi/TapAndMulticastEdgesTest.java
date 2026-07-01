package dev.nthings.otlp4j.transport.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.processor.OverflowPolicy;
import dev.nthings.otlp4j.receiver.TapOptions;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.FlowSubscribers;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ReceiverTap and MulticastPublisher edge cases")
class TapAndMulticastEdgesTest {

    @DisplayName("A typed channel multicasts to every subscriber")
    @Test
    void receiverTapMulticastsToEverySubscriber() {
        var tap = new ReceiverTap();
        try {
            var first = new CopyOnWriteArrayList<TracesData>();
            var second = new CopyOnWriteArrayList<TracesData>();
            tap.traces().subscribe(FlowSubscribers.recording(first));
            tap.traces().subscribe(FlowSubscribers.recording(second));
            tap.publishTraces(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)));
            await().atMost(Duration.ofSeconds(2)).until(() -> first.size() >= 1 && second.size() >= 1);
            assertThat(first).hasSize(1);
            assertThat(second).hasSize(1);
        } finally {
            tap.close();
        }
    }

    @DisplayName("droppedCount increases when a subscriber stalls")
    @Test
    void droppedCountIncrementsWhenSubscriberStalls() {
        var tap = new ReceiverTap();
        try {
            tap.traces(new TapOptions(OverflowPolicy.DROP_NEWEST, 1)).subscribe(FlowSubscribers.noOp());
            for (var i = 0; i < 50; i++) {
                tap.publishTraces(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)));
            }
            assertThat(tap.droppedCount()).isGreaterThan(0L);
        } finally {
            tap.close();
        }
    }

    @DisplayName("Closed publisher completes new subscribers immediately")
    @Test
    void closingPublisherCompletesNewSubscribersImmediately() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        pub.close();
        var completed = new AtomicBoolean();
        pub.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription s) { s.request(1); }
            @Override public void onNext(String item) {}
            @Override public void onError(Throwable t) {}
            @Override public void onComplete() { completed.set(true); }
        });
        assertThat(completed.get()).isTrue();
    }

    @DisplayName("Cancelling a subscription stops further delivery")
    @Test
    void cancellingSubscriptionStopsDelivery() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        try {
            var received = new CopyOnWriteArrayList<String>();
            var subRef = new AtomicReference<Flow.Subscription>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) {
                    subRef.set(s);
                    s.request(100);
                }
                @Override public void onNext(String item) { received.add(item); }
                @Override public void onError(Throwable t) {}
                @Override public void onComplete() {}
            });
            pub.publish("a");
            await().atMost(Duration.ofSeconds(2)).until(() -> !received.isEmpty());
            subRef.get().cancel();
            var sizeAtCancel = received.size();
            for (var i = 0; i < 5; i++) {
                pub.publish("after-cancel-" + i);
            }
            await().pollDelay(Duration.ofMillis(150))
                    .atMost(Duration.ofSeconds(1))
                    .until(() -> received.size() == sizeAtCancel);
            assertThat(received.size()).isEqualTo(sizeAtCancel);
        } finally {
            pub.close();
        }
    }

    @DisplayName("Requesting zero items signals IllegalArgumentException")
    @Test
    void requestZeroIsRejected() {
        var drops = new LongAdder();
        var pub = new MulticastPublisher<String>(drops);
        try {
            var error = new AtomicReference<Throwable>();
            pub.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { s.request(0); }
                @Override public void onNext(String item) {}
                @Override public void onError(Throwable t) { error.set(t); }
                @Override public void onComplete() {}
            });
            await().atMost(Duration.ofSeconds(2)).until(() -> error.get() != null);
            assertThat(error.get()).isInstanceOf(IllegalArgumentException.class);
        } finally {
            pub.close();
        }
    }

    @DisplayName("TapOptions.defaults uses DROP_OLDEST with buffer 256")
    @Test
    void tapOptionsDefaultsAreSane() {
        var opts = TapOptions.defaults();
        assertThat(opts.strategy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
        assertThat(opts.bufferSize()).isEqualTo(256);
    }

    @DisplayName("TapOptions rejects a non-positive buffer size")
    @Test
    void tapOptionsRejectsBadBuffer() {
        assertThatThrownBy(() -> new TapOptions(OverflowPolicy.BLOCK, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
