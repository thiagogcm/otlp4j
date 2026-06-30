package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.Source;
import dev.nthings.otlp4j.core.TraceSink;
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

/// Shared machinery for the OTLP receivers: per-signal [Source]s, the live [TelemetryTap], and the
/// dispatch wiring that feeds decoded batches to subscribers while mirroring them to the tap.
///
/// A concrete subclass (in a transport module) supplies a factory turning the [Dispatchers] built
/// here into its transport [OtlpServer], plus a display name for logs; the rest lives here.
public abstract class AbstractOtlpReceiver implements Receiver {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpReceiver.class);

    private final SignalSource<TracesData>    traces   = new SignalSource<>(TracesData.class);
    private final SignalSource<MetricsData>  metrics  = new SignalSource<>(MetricsData.class);
    private final SignalSource<LogsData>     logs     = new SignalSource<>(LogsData.class);
    private final SignalSource<ProfilesData> profiles = new SignalSource<>(ProfilesData.class);
    private final ReceiverTap tap = new ReceiverTap();
    private final OtlpServer server;
    private final String transportName;

    /// Builds the dispatch wiring, then asks `serverFactory` for the transport server bound to it.
    /// `transportName` (e.g. `OTLP/gRPC`) is used only for log messages.
    protected AbstractOtlpReceiver(
            String transportName,
            Function<Dispatchers, OtlpServer> serverFactory,
            @Nullable TraceSink onTraces,
            @Nullable MetricSink onMetrics,
            @Nullable LogSink onLogs,
            @Nullable ProfileSink onProfiles) {
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
    public final Source<TracesData> traces() { return traces; }

    @Override
    public final Source<MetricsData> metrics() { return metrics; }

    @Override
    public final Source<LogsData> logs() { return logs; }

    @Override
    public final Source<ProfilesData> profiles() { return profiles; }

    @Override
    public final TelemetryTap tap() { return tap; }

    /// Starts the underlying transport, throwing [UncheckedIOException] if the bind fails. Concrete
    /// receivers call this from their covariant [#start()] override and return `this`.
    protected final void startServer() {
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start " + transportName + " receiver", e);
        }
        log.debug("{} receiver started on port {}", transportName, server.port());
    }

    @Override
    public final int port() {
        return server.port();
    }

    @Override
    public final CompletionStage<Void> shutdown(Duration timeout) {
        // Graceful: drain the server first, then close the tap. Closing it up front makes
        // MulticastPublisher.publish early-return, dropping telemetry accepted during the drain.
        return server.shutdown(timeout).whenComplete((v, t) -> tap.close());
    }

    @Override
    public final CompletionStage<Void> shutdownNow() {
        // Forceful: nothing further will be accepted, so detach subscribers immediately.
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
