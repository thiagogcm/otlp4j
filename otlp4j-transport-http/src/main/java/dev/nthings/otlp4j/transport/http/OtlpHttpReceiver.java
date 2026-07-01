package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.transport.spi.ServerReceiver;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpServer;
import java.time.Duration;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Builds [Receiver]s that accept OTLP/HTTP requests and dispatch to per-signal sinks.
///
/// Call [Receiver#start()] on the built receiver to bind the transport. The default bind is
/// `localhost:4318` with plaintext.
public final class OtlpHttpReceiver {

    /// The conventional OTLP/HTTP port (4318); gRPC uses 4317.
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpReceiver() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Builder for an OTLP/HTTP [Receiver]. Defaults bind `localhost:4318` with plaintext transport.
    public static final class Builder {

        private ServerConfig.Builder config =
                ServerConfig.builder().setPort(DEFAULT_HTTP_PORT);
        private @Nullable Sink<? super TracesData>   traces;
        private @Nullable Sink<? super MetricsData>  metrics;
        private @Nullable Sink<? super LogsData>     logs;
        private @Nullable Sink<? super ProfilesData> profiles;

        private Builder() {}

        /// Replaces the whole transport config. The supplied port is used verbatim (the 4318
        /// default applies only to the unconfigured builder).
        public Builder setConfig(ServerConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        public Builder setEndpoint(String bindHost, int port) {
            config.setBindHost(bindHost).setPort(port);
            return this;
        }

        public Builder setPort(int port) {
            config.setPort(port);
            return this;
        }

        public Builder ephemeralPort() {
            return setPort(0);
        }

        /// Selects the server TLS mode. Defaults to plaintext.
        public Builder setTls(Tls tls) {
            config.setTls(tls);
            return this;
        }

        /// Caps a single decoded export request. Defaults to 4 MiB.
        public Builder setMaxInboundMessageSizeBytes(int bytes) {
            config.setMaxInboundMessageSizeBytes(bytes);
            return this;
        }

        /// Bounds the transport/TLS handshake deadline, not request body or connection idle time.
        /// Defaults to 20s.
        public Builder setHandshakeTimeout(Duration handshakeTimeout) {
            config.setHandshakeTimeout(handshakeTimeout);
            return this;
        }

        /// Supplies the executor that runs admitted requests; a bounded pool caps concurrent work.
        public Builder setServerExecutor(Executor serverExecutor) {
            config.setServerExecutor(serverExecutor);
            return this;
        }

        public Builder onTraces(Sink<? super TracesData> sink) {
            this.traces = sink;
            return this;
        }

        public Builder onMetrics(Sink<? super MetricsData> sink) {
            this.metrics = sink;
            return this;
        }

        public Builder onLogs(Sink<? super LogsData> sink) {
            this.logs = sink;
            return this;
        }

        public Builder onProfiles(Sink<? super ProfilesData> sink) {
            this.profiles = sink;
            return this;
        }

        public Receiver build() {
            return new ServerReceiver("OTLP/HTTP", disp -> new HttpOtlpServer(config.build(), disp),
                    traces, metrics, logs, profiles);
        }
    }
}
