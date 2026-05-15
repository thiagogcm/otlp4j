package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;

/// A [TelemetryConsumer] that forwards every signal to a delegate.
///
/// Use this for pipeline stages that transform only some signals; unoverridden methods keep
/// telemetry flowing downstream instead of accepting and dropping it.
public class ForwardingConsumer implements TelemetryConsumer {

    private final TelemetryConsumer delegate;

    public ForwardingConsumer(TelemetryConsumer delegate) {
        this.delegate = delegate;
    }

    /// The downstream consumer this stage forwards to.
    protected final TelemetryConsumer delegate() {
        return delegate;
    }

    @Override
    public ExportResult consumeTraces(TraceData traces) {
        return delegate.consumeTraces(traces);
    }

    @Override
    public ExportResult consumeMetrics(MetricsData metrics) {
        return delegate.consumeMetrics(metrics);
    }

    @Override
    public ExportResult consumeLogs(LogsData logs) {
        return delegate.consumeLogs(logs);
    }

    @Override
    public ExportResult consumeProfiles(ProfilesData profiles) {
        return delegate.consumeProfiles(profiles);
    }
}
