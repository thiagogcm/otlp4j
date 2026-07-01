package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;
import dev.nthings.otlp4j.pipeline.ProfilesSink;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.receiver.TelemetryTap;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared [Receiver] implementation backed by a transport [OtlpServer]. Exposes per-signal
/// [Source]s and a [TelemetryTap], dispatches decoded batches to subscribers while mirroring
/// to the tap. gRPC and HTTP entry points compose one of these.
public final class ServerReceiver implements Receiver {

    private static final Logger log = LoggerFactory.getLogger(ServerReceiver.class);

    private final SignalSource<TracesData>    traces   = new SignalSource<>(TracesData.class);
    private final SignalSource<MetricsData>  metrics  = new SignalSource<>(MetricsData.class);
    private final SignalSource<LogsData>     logs     = new SignalSource<>(LogsData.class);
    private final SignalSource<ProfilesData> profiles = new SignalSource<>(ProfilesData.class);
    private final ReceiverTap tap = new ReceiverTap();
    private final OtlpServer server;
    private final String transportName;

    /// Builds dispatch wiring, then asks `serverFactory` for the transport server bound to it.
    /// `transportName` (e.g. `OTLP/gRPC`) is used only for log messages.
    public ServerReceiver(
            String transportName,
            Function<Dispatchers, OtlpServer> serverFactory,
            @Nullable TracesSink onTraces,
            @Nullable MetricsSink onMetrics,
            @Nullable LogsSink onLogs,
            @Nullable ProfilesSink onProfiles) {
        this.transportName = transportName;
        var dispatchers = new Dispatchers(
                this::dispatchTraces,
                this::dispatchMetrics,
                this::dispatchLogs,
                this::dispatchProfiles);
        this.server = serverFactory.apply(dispatchers);
        if (onTraces != null)   traces.subscribe(onTraces);
        if (onMetrics != null)  metrics.subscribe(onMetrics);
        if (onLogs != null)     logs.subscribe(onLogs);
        if (onProfiles != null) profiles.subscribe(onProfiles);
    }

    @Override
    public Source<TracesData> traces() { return traces; }

    @Override
    public Source<MetricsData> metrics() { return metrics; }

    @Override
    public Source<LogsData> logs() { return logs; }

    @Override
    public Source<ProfilesData> profiles() { return profiles; }

    @Override
    public TelemetryTap tap() { return tap; }

    @Override
    public Receiver start() {
        // Starts the underlying transport; throws [UncheckedIOException] if bind fails.
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start " + transportName + " receiver", e);
        }
        log.debug("{} receiver started on port {}", transportName, server.port());
        return this;
    }

    @Override
    public int port() {
        return server.port();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        // Graceful: drain server first, then close tap. Closing it up front makes
        // [MulticastPublisher#publish] early-return, dropping telemetry accepted during drain.
        return server.shutdown(timeout).whenComplete((v, t) -> tap.close());
    }

    @Override
    public CompletionStage<Void> shutdownNow() {
        // Forceful: nothing further is accepted, so detach subscribers immediately.
        tap.close();
        return server.shutdownNow();
    }

    private CompletionStage<ConsumeResult<TracesData>> dispatchTraces(TracesData batch) {
        tap.publishTraces(batch);
        return traces.dispatch(batch);
    }

    private CompletionStage<ConsumeResult<MetricsData>> dispatchMetrics(MetricsData batch) {
        tap.publishMetrics(batch);
        return metrics.dispatch(batch);
    }

    private CompletionStage<ConsumeResult<LogsData>> dispatchLogs(LogsData batch) {
        tap.publishLogs(batch);
        return logs.dispatch(batch);
    }

    private CompletionStage<ConsumeResult<ProfilesData>> dispatchProfiles(ProfilesData batch) {
        tap.publishProfiles(batch);
        return profiles.dispatch(batch);
    }
}
