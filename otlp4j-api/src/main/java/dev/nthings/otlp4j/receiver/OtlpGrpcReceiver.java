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
import dev.nthings.otlp4j.receiver.internal.ReceiverTap;
import dev.nthings.otlp4j.receiver.internal.SignalSource;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Receives OTLP/gRPC requests, dispatches them to per-signal consumers, and exposes a
/// [TelemetryTap] for live observation.
///
/// `.onTraces(...)`-style builder sugar attaches a single consumer per signal; richer graphs
/// (branches, fan-out) wire the [Source]s via `Pipeline.from(receiver.traces()) ...`.
public final class OtlpGrpcReceiver implements Receiver {

    private static final Logger log = LoggerFactory.getLogger(OtlpGrpcReceiver.class);

    private final SignalSource<TraceData>    traces   = new SignalSource<>(TraceData.class);
    private final SignalSource<MetricsData>  metrics  = new SignalSource<>(MetricsData.class);
    private final SignalSource<LogsData>     logs     = new SignalSource<>(LogsData.class);
    private final SignalSource<ProfilesData> profiles = new SignalSource<>(ProfilesData.class);
    private final ReceiverTap tap = new ReceiverTap();
    private final OtlpServer server;

    private OtlpGrpcReceiver(Builder b) {
        var dispatchers = new OtlpServerProvider.Dispatchers(
                this::dispatchTraces,
                this::dispatchMetrics,
                this::dispatchLogs,
                this::dispatchProfiles);
        this.server = SpiSupport.firstProvider(OtlpServerProvider.class).create(b.config, dispatchers);
        if (b.traces != null)   traces.consume(b.traces);
        if (b.metrics != null)  metrics.consume(b.metrics);
        if (b.logs != null)     logs.consume(b.logs);
        if (b.profiles != null) profiles.consume(b.profiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Source<TraceData> traces() { return traces; }

    @Override
    public Source<MetricsData> metrics() { return metrics; }

    @Override
    public Source<LogsData> logs() { return logs; }

    @Override
    public Source<ProfilesData> profiles() { return profiles; }

    @Override
    public TelemetryTap tap() { return tap; }

    @Override
    public OtlpGrpcReceiver start() {
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start OTLP/gRPC receiver", e);
        }
        log.debug("OTLP/gRPC receiver started on port {}", server.port());
        return this;
    }

    @Override
    public int port() {
        return server.port();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        tap.close();
        return server.shutdown(timeout);
    }

    @Override
    public CompletionStage<Void> shutdownNow() {
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

    /// Builder for [OtlpGrpcReceiver]. Defaults bind `0.0.0.0:4317` with plaintext transport.
    public static final class Builder {

        private ServerTransportConfig config = ServerTransportConfig.builder().build();
        private TraceConsumer    traces;
        private MetricConsumer   metrics;
        private LogConsumer      logs;
        private ProfileConsumer  profiles;

        private Builder() {}

        public Builder transport(ServerTransportConfig config) {
            this.config = config;
            return this;
        }

        public Builder endpoint(String bindHost, int port) {
            this.config = ServerTransportConfig.builder()
                    .bindHost(bindHost)
                    .port(port)
                    .tls(this.config.tls())
                    .build();
            return this;
        }

        public Builder port(int port) {
            return endpoint(config.bindHost(), port);
        }

        public Builder ephemeralPort() {
            return port(0);
        }

        public Builder onTraces(TraceConsumer consumer) {
            this.traces = consumer;
            return this;
        }

        public Builder onMetrics(MetricConsumer consumer) {
            this.metrics = consumer;
            return this;
        }

        public Builder onLogs(LogConsumer consumer) {
            this.logs = consumer;
            return this;
        }

        public Builder onProfiles(ProfileConsumer consumer) {
            this.profiles = consumer;
            return this;
        }

        public OtlpGrpcReceiver build() {
            return new OtlpGrpcReceiver(this);
        }
    }
}
