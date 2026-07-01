package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.pipeline.LogSink;
import dev.nthings.otlp4j.pipeline.MetricSink;
import dev.nthings.otlp4j.pipeline.ProfileSink;
import dev.nthings.otlp4j.pipeline.TraceSink;
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

    /// Builds (but does not start) a receiver bound to `port` on the default loopback host.
    public static Receiver on(int port) {
        return builder().setPort(port).build();
    }

    /// Builds (but does not start) a receiver bound to `bindHost:port`.
    public static Receiver on(String bindHost, int port) {
        return builder().setEndpoint(bindHost, port).build();
    }

    /// Builder for an OTLP/HTTP [Receiver]. Defaults bind `localhost:4318` with plaintext transport.
    public static final class Builder {

        private ServerConfig.Builder config =
                ServerConfig.builder().setPort(DEFAULT_HTTP_PORT);
        private @Nullable TraceSink    traces;
        private @Nullable MetricSink   metrics;
        private @Nullable LogSink      logs;
        private @Nullable ProfileSink  profiles;

        private Builder() {}

        /// Replaces the whole transport config. The supplied port is used verbatim (the 4318
        /// default applies only to the unconfigured builder).
        public Builder transport(ServerConfig config) {
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

        public Builder onTraces(TraceSink sink) {
            this.traces = sink;
            return this;
        }

        public Builder onMetrics(MetricSink sink) {
            this.metrics = sink;
            return this;
        }

        public Builder onLogs(LogSink sink) {
            this.logs = sink;
            return this;
        }

        public Builder onProfiles(ProfileSink sink) {
            this.profiles = sink;
            return this;
        }

        public Receiver build() {
            return new ServerReceiver("OTLP/HTTP", disp -> new HttpOtlpServer(config.build(), disp),
                    traces, metrics, logs, profiles);
        }
    }
}
