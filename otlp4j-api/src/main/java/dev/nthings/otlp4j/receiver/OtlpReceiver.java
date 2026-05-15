package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.api.internal.SpiSupport;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import java.io.IOException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Receives OTLP exports and feeds typed telemetry into a [TelemetryConsumer].
///
/// The server implementation is supplied by an [OtlpServerProvider] discovered through [SpiSupport].
/// Add `otlp4j-transport` at runtime for the built-in OTLP/gRPC transport. Signals with no
/// configured handler are accepted with [ExportResult#success()].
public final class OtlpReceiver {

    private static final Logger log = LoggerFactory.getLogger(OtlpReceiver.class);

    private final OtlpServer server;

    private OtlpReceiver(Builder builder) {
        var consumer =
                builder.consumer != null
                        ? builder.consumer
                        : fromHandlers(
                                builder.traceHandler,
                                builder.metricsHandler,
                                builder.logsHandler,
                                builder.profilesHandler);
        this.server = SpiSupport.provider(OtlpServerProvider.class).create(consumer);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Starts the receiver on the given TCP port; pass `0` for an ephemeral port.
    public OtlpReceiver start(int port) throws IOException {
        server.start(port);
        log.debug("OTLP receiver started on port {}", server.port());
        return this;
    }

    /// The port the receiver is listening on.
    public int port() {
        return server.port();
    }

    /// Initiates a graceful shutdown; in-flight exports are allowed to complete.
    public OtlpReceiver shutdown() {
        server.shutdown();
        return this;
    }

    /// Initiates a forceful shutdown; in-flight exports are cancelled.
    public OtlpReceiver shutdownNow() {
        server.shutdownNow();
        return this;
    }

    /// Blocks until the receiver has terminated, or the timeout elapses.
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return server.awaitTermination(timeout);
    }

    /// Blocks until the receiver has terminated.
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    private static TelemetryConsumer fromHandlers(
            TraceHandler traceHandler,
            MetricsHandler metricsHandler,
            LogsHandler logsHandler,
            ProfilesHandler profilesHandler) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                return traceHandler != null ? traceHandler.onTraces(traces) : ExportResult.success();
            }

            @Override
            public ExportResult consumeMetrics(MetricsData metrics) {
                return metricsHandler != null
                        ? metricsHandler.onMetrics(metrics)
                        : ExportResult.success();
            }

            @Override
            public ExportResult consumeLogs(LogsData logs) {
                return logsHandler != null ? logsHandler.onLogs(logs) : ExportResult.success();
            }

            @Override
            public ExportResult consumeProfiles(ProfilesData profiles) {
                return profilesHandler != null
                        ? profilesHandler.onProfiles(profiles)
                        : ExportResult.success();
            }
        };
    }

    /// Builds an [OtlpReceiver]. Set a [#consumer] or any subset of per-signal handlers.
    public static final class Builder {

        private TelemetryConsumer consumer;
        private TraceHandler traceHandler;
        private MetricsHandler metricsHandler;
        private LogsHandler logsHandler;
        private ProfilesHandler profilesHandler;

        private Builder() {}

        /// Sets the consumer that receives every signal — a pipeline, connector, or exporter. When
        /// set, this takes precedence over any per-signal handlers.
        public Builder consumer(TelemetryConsumer consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder traceHandler(TraceHandler handler) {
            this.traceHandler = handler;
            return this;
        }

        public Builder metricsHandler(MetricsHandler handler) {
            this.metricsHandler = handler;
            return this;
        }

        public Builder logsHandler(LogsHandler handler) {
            this.logsHandler = handler;
            return this;
        }

        public Builder profilesHandler(ProfilesHandler handler) {
            this.profilesHandler = handler;
            return this;
        }

        public OtlpReceiver build() {
            return new OtlpReceiver(this);
        }
    }
}
