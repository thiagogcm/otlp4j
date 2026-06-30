package dev.nthings.otlp4j.testing;

import java.util.List;
import java.util.concurrent.Flow;

/// Reusable [Flow.Subscriber] stand-ins for tests against multicast publishers.
public final class FlowSubscribers {

    private FlowSubscribers() {}

    /// Records every onNext value into sink and requests initialDemand items on subscribe.
    public static <T> Flow.Subscriber<T> recording(List<T> sink, long initialDemand) {
        return new Recording<>(sink, initialDemand);
    }

    /// Records with [Long.MAX_VALUE] initial demand.
    public static <T> Flow.Subscriber<T> recording(List<T> sink) {
        return recording(sink, Long.MAX_VALUE);
    }

    /// A subscriber that never requests and discards every callback. Useful for exercising
    /// publisher back-pressure paths.
    public static <T> Flow.Subscriber<T> noOp() {
        return new NoOp<>();
    }

    private record Recording<T>(List<T> sink, long initialDemand) implements Flow.Subscriber<T> {
        @Override public void onSubscribe(Flow.Subscription s) { s.request(initialDemand); }
        @Override public void onNext(T item) { sink.add(item); }
        @Override public void onError(Throwable t) {}
        @Override public void onComplete() {}
    }

    private static final class NoOp<T> implements Flow.Subscriber<T> {
        @Override public void onSubscribe(Flow.Subscription s) {}
        @Override public void onNext(T item) {}
        @Override public void onError(Throwable t) {}
        @Override public void onComplete() {}
    }
}
