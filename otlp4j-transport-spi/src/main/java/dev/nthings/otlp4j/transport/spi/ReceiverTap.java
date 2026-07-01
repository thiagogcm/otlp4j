package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.receiver.TapOptions;
import dev.nthings.otlp4j.receiver.TelemetryTap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.LongAdder;

/// [TelemetryTap] implementation backed by per-signal [MulticastPublisher]s.
final class ReceiverTap implements TelemetryTap, AutoCloseable {

    private final LongAdder drops = new LongAdder();
    private final MulticastPublisher<TracesData>    traces    = new MulticastPublisher<>(drops);
    private final MulticastPublisher<MetricsData>  metrics   = new MulticastPublisher<>(drops);
    private final MulticastPublisher<LogsData>     logs      = new MulticastPublisher<>(drops);
    private final MulticastPublisher<ProfilesData> profiles  = new MulticastPublisher<>(drops);

    @Override
    public Flow.Publisher<TracesData> traces() {
        return traces;
    }

    @Override
    public Flow.Publisher<TracesData> traces(TapOptions options) {
        return subscriber -> traces.subscribe(subscriber, options);
    }

    @Override
    public Flow.Publisher<MetricsData> metrics() {
        return metrics;
    }

    @Override
    public Flow.Publisher<MetricsData> metrics(TapOptions options) {
        return subscriber -> metrics.subscribe(subscriber, options);
    }

    @Override
    public Flow.Publisher<LogsData> logs() {
        return logs;
    }

    @Override
    public Flow.Publisher<LogsData> logs(TapOptions options) {
        return subscriber -> logs.subscribe(subscriber, options);
    }

    @Override
    public Flow.Publisher<ProfilesData> profiles() {
        return profiles;
    }

    @Override
    public Flow.Publisher<ProfilesData> profiles(TapOptions options) {
        return subscriber -> profiles.subscribe(subscriber, options);
    }

    @Override
    public long droppedCount() {
        return drops.sum();
    }

    public void publishTraces(TracesData batch) {
        traces.publish(batch);
    }

    public void publishMetrics(MetricsData batch) {
        metrics.publish(batch);
    }

    public void publishLogs(LogsData batch) {
        logs.publish(batch);
    }

    public void publishProfiles(ProfilesData batch) {
        profiles.publish(batch);
    }

    @Override
    public void close() {
        traces.close();
        metrics.close();
        logs.close();
        profiles.close();
    }
}
