package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.api.internal.SpiSupport;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.LogConsumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.ProfileConsumer;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.Protocol;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared machinery for the OTLP receivers: per-signal [Source]s, the live [TelemetryTap], and the
/// dispatch wiring that feeds decoded batches to subscribers while mirroring them to the tap.
///
/// The concrete [OtlpGrpcReceiver] / [OtlpHttpReceiver] differ only in the [Protocol] they resolve a
/// transport [OtlpServer] for, and in the concrete type their [#start()] returns; the rest lives
/// here.
sealed abstract class AbstractOtlpReceiver implements Receiver
        permits OtlpGrpcReceiver, OtlpHttpReceiver {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpReceiver.class);

    private final SignalSource<TraceData>    traces   = new SignalSource<>(TraceData.class);
    private final SignalSource<MetricsData>  metrics  = new SignalSource<>(MetricsData.class);
    private final SignalSource<LogsData>     logs     = new SignalSource<>(LogsData.class);
    private final SignalSource<ProfilesData> profiles = new SignalSource<>(ProfilesData.class);
    private final ReceiverTap tap = new ReceiverTap();
    private final OtlpServer server;
    private final String transportName;

    AbstractOtlpReceiver(
            ServerTransportConfig config,
            Protocol protocol,
            TraceConsumer onTraces,
            MetricConsumer onMetrics,
            LogConsumer onLogs,
            ProfileConsumer onProfiles) {
        this.transportName = displayName(protocol);
        var dispatchers = new OtlpServerProvider.Dispatchers(
                this::dispatchTraces,
                this::dispatchMetrics,
                this::dispatchLogs,
                this::dispatchProfiles);
        this.server = SpiSupport.provider(OtlpServerProvider.class, protocol).create(config, dispatchers);
        if (onTraces != null)   traces.subscribe(onTraces);
        if (onMetrics != null)  metrics.subscribe(onMetrics);
        if (onLogs != null)     logs.subscribe(onLogs);
        if (onProfiles != null) profiles.subscribe(onProfiles);
    }

    private static String displayName(Protocol protocol) {
        return switch (protocol) {
            case GRPC -> "OTLP/gRPC";
            case HTTP_PROTOBUF -> "OTLP/HTTP";
        };
    }

    @Override
    public final Source<TraceData> traces() { return traces; }

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

    private CompletionStage<ConsumeResult<TraceData>> dispatchTraces(TraceData batch) {
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
