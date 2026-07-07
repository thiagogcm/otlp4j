package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import java.util.concurrent.Flow;

/// A live, non-blocking side-channel that emits a copy of every batch a receiver accepts.
///
/// Tap subscribers are independent of the in-pipeline path: a lagging tap subscriber never affects
/// the OTLP acknowledgement sent back to senders. Each `Flow.Publisher` multicasts with
/// per-subscription back-pressure. The no-arg publishers use [TapOptions#defaults()]; the
/// [TapOptions] overloads bind buffer size and overflow policy to that publisher's next
/// subscription, so options travel with the subscription instead of shared tap state.
public interface TelemetryTap {

    /// Live batches of `signal` under [TapOptions#defaults()].
    <T> Flow.Publisher<T> publisher(SignalDescriptor<T> signal);

    /// Live batches of `signal`; `options` bind to the next subscription.
    <T> Flow.Publisher<T> publisher(SignalDescriptor<T> signal, TapOptions options);

    /// Live trace batches under [TapOptions#defaults()].
    default Flow.Publisher<TracesData> traces() {
        return publisher(SignalDescriptor.TRACES);
    }

    /// Live trace batches; `options` bind to the next subscription.
    default Flow.Publisher<TracesData> traces(TapOptions options) {
        return publisher(SignalDescriptor.TRACES, options);
    }

    /// Live metric batches under [TapOptions#defaults()].
    default Flow.Publisher<MetricsData> metrics() {
        return publisher(SignalDescriptor.METRICS);
    }

    /// Live metric batches; `options` bind to the next subscription.
    default Flow.Publisher<MetricsData> metrics(TapOptions options) {
        return publisher(SignalDescriptor.METRICS, options);
    }

    /// Live log batches under [TapOptions#defaults()].
    default Flow.Publisher<LogsData> logs() {
        return publisher(SignalDescriptor.LOGS);
    }

    /// Live log batches; `options` bind to the next subscription.
    default Flow.Publisher<LogsData> logs(TapOptions options) {
        return publisher(SignalDescriptor.LOGS, options);
    }

    /// Live profile batches under [TapOptions#defaults()].
    default Flow.Publisher<ProfilesData> profiles() {
        return publisher(SignalDescriptor.PROFILES);
    }

    /// Live profile batches; `options` bind to the next subscription.
    default Flow.Publisher<ProfilesData> profiles(TapOptions options) {
        return publisher(SignalDescriptor.PROFILES, options);
    }

    /// Total number of batches dropped across every tap publisher since the receiver started.
    long droppedCount();
}
