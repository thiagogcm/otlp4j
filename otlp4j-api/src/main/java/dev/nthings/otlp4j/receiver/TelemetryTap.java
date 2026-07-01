package dev.nthings.otlp4j.receiver;

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

    /// Live trace batches under [TapOptions#defaults()].
    Flow.Publisher<TracesData> traces();

    /// Live trace batches; `options` bind to the next subscription.
    Flow.Publisher<TracesData> traces(TapOptions options);

    /// Live metric batches under [TapOptions#defaults()].
    Flow.Publisher<MetricsData> metrics();

    /// Live metric batches; `options` bind to the next subscription.
    Flow.Publisher<MetricsData> metrics(TapOptions options);

    /// Live log batches under [TapOptions#defaults()].
    Flow.Publisher<LogsData> logs();

    /// Live log batches; `options` bind to the next subscription.
    Flow.Publisher<LogsData> logs(TapOptions options);

    /// Live profile batches under [TapOptions#defaults()].
    Flow.Publisher<ProfilesData> profiles();

    /// Live profile batches; `options` bind to the next subscription.
    Flow.Publisher<ProfilesData> profiles(TapOptions options);

    /// Total number of batches dropped across every tap publisher since the receiver started.
    long droppedCount();
}
