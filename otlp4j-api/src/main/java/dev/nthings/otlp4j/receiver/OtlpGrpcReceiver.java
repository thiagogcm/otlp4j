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
import dev.nthings.otlp4j.spi.Tls;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
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
        this.server = SpiSupport.firstProvider(OtlpServerProvider.class).create(b.config.build(), dispatchers);
        if (b.traces != null)   traces.subscribe(b.traces);
        if (b.metrics != null)  metrics.subscribe(b.metrics);
        if (b.logs != null)     logs.subscribe(b.logs);
        if (b.profiles != null) profiles.subscribe(b.profiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Builds — but does not [#start()] — a receiver bound to `port` on all interfaces. Mirrors the
    /// exporter's `OtlpGrpcExporter.to(String, int)` convenience; wire
    /// consumers through the per-signal [Source]s, then call [#start()].
    public static OtlpGrpcReceiver on(int port) {
        return builder().port(port).build();
    }

    /// Builds — but does not [#start()] — a receiver bound to `bindHost:port`.
    public static OtlpGrpcReceiver on(String bindHost, int port) {
        return builder().endpoint(bindHost, port).build();
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
        // Graceful: drain the server first, then close the tap. Closing it up front makes
        // MulticastPublisher.publish early-return, dropping telemetry accepted during the drain.
        return server.shutdown(timeout).whenComplete((v, t) -> tap.close());
    }

    @Override
    public CompletionStage<Void> shutdownNow() {
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

    /// Builder for [OtlpGrpcReceiver]. Defaults bind `0.0.0.0:4317` with plaintext transport.
    public static final class Builder {

        private ServerTransportConfig.Builder config = ServerTransportConfig.builder();
        private TraceConsumer    traces;
        private MetricConsumer   metrics;
        private LogConsumer      logs;
        private ProfileConsumer  profiles;

        private Builder() {}

        public Builder transport(ServerTransportConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        public Builder endpoint(String bindHost, int port) {
            config.bindHost(bindHost).port(port);
            return this;
        }

        public Builder port(int port) {
            config.port(port);
            return this;
        }

        public Builder ephemeralPort() {
            return port(0);
        }

        /// Selects the server TLS mode (e.g. [Tls#custom] to serve TLS). Defaults to plaintext.
        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        /// Caps a single decoded export request; guards against memory-exhausting oversized
        /// requests. Defaults to 4 MiB.
        public Builder maxInboundMessageSizeBytes(int bytes) {
            config.maxInboundMessageSizeBytes(bytes);
            return this;
        }

        /// Caps in-flight calls per connection; `0` (the default) leaves it unlimited.
        public Builder maxConcurrentCallsPerConnection(int max) {
            config.maxConcurrentCallsPerConnection(max);
            return this;
        }

        /// Bounds the transport/TLS handshake only — not a slow request body or idle connection.
        /// Defaults to 20s.
        public Builder handshakeTimeout(Duration handshakeTimeout) {
            config.handshakeTimeout(handshakeTimeout);
            return this;
        }

        /// Supplies the executor that runs admitted calls; a bounded pool caps concurrent work.
        /// Defaults to gRPC's own executor.
        public Builder serverExecutor(Executor serverExecutor) {
            config.serverExecutor(serverExecutor);
            return this;
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
