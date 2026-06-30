package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.core.Telemetry;
import java.util.concurrent.Flow;

/// A live, non-blocking side-channel that emits a copy of every batch a receiver accepts.
///
/// Tap subscribers are independent of the in-pipeline path: a tap subscriber that lags never
/// affects the OTLP acknowledgement sent back to senders. Each `Flow.Publisher` returned by
/// this interface multicasts to its subscribers with per-subscription back-pressure controlled
/// by the [TapOptions] passed at subscription time.
public interface TelemetryTap {

    /// Live trace batches.
    Flow.Publisher<TracesData>    traces();

    /// Live metric batches.
    Flow.Publisher<MetricsData>  metrics();

    /// Live log batches.
    Flow.Publisher<LogsData>     logs();

    /// Live profile batches.
    Flow.Publisher<ProfilesData> profiles();

    /// All four signals through a single sealed envelope.
    Flow.Publisher<Telemetry>    all();

    /// Tap subscription options used by the next `subscribe(...)` call on any publisher returned
    /// from this tap. Calls are thread-safe but not transactional; setting a value affects only
    /// subscriptions registered after the call.
    void setOptions(TapOptions options);

    /// Total number of batches dropped across every tap publisher since the receiver started.
    long droppedCount();
}
