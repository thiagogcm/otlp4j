package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.receiver.Telemetry;
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
    private final MulticastPublisher<Telemetry>    all       = new MulticastPublisher<>(drops);

    @Override
    public Flow.Publisher<TracesData> traces() {
        return traces;
    }

    @Override
    public Flow.Publisher<MetricsData> metrics() {
        return metrics;
    }

    @Override
    public Flow.Publisher<LogsData> logs() {
        return logs;
    }

    @Override
    public Flow.Publisher<ProfilesData> profiles() {
        return profiles;
    }

    @Override
    public Flow.Publisher<Telemetry> all() {
        return all;
    }

    @Override
    public void setOptions(TapOptions options) {
        traces.setOptions(options);
        metrics.setOptions(options);
        logs.setOptions(options);
        profiles.setOptions(options);
        all.setOptions(options);
    }

    @Override
    public long droppedCount() {
        return drops.sum();
    }

    public void publishTraces(TracesData batch) {
        traces.publish(batch);
        if (all.hasSubscribers()) all.publish(new Telemetry.Traces(batch));
    }

    public void publishMetrics(MetricsData batch) {
        metrics.publish(batch);
        if (all.hasSubscribers()) all.publish(new Telemetry.Metrics(batch));
    }

    public void publishLogs(LogsData batch) {
        logs.publish(batch);
        if (all.hasSubscribers()) all.publish(new Telemetry.Logs(batch));
    }

    public void publishProfiles(ProfilesData batch) {
        profiles.publish(batch);
        if (all.hasSubscribers()) all.publish(new Telemetry.Profiles(batch));
    }

    @Override
    public void close() {
        traces.close();
        metrics.close();
        logs.close();
        profiles.close();
        all.close();
    }
}
