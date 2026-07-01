package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;
import dev.nthings.otlp4j.pipeline.ProfilesSink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.transport.spi.ServerReceiver;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpServer;
import java.time.Duration;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Builds [Receiver]s that accept OTLP/gRPC requests and dispatch to per-signal sinks.
///
/// Call [Receiver#start()] on the built receiver to bind the transport. The default bind is
/// `localhost:4317`.
public final class OtlpGrpcReceiver {

    private OtlpGrpcReceiver() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Builder for an OTLP/gRPC [Receiver]. Defaults bind `localhost:4317` with plaintext transport.
    public static final class Builder {

        private ServerConfig.Builder config = ServerConfig.builder();
        private @Nullable TracesSink    traces;
        private @Nullable MetricsSink   metrics;
        private @Nullable LogsSink      logs;
        private @Nullable ProfilesSink  profiles;

        private Builder() {}

        /// Replaces the whole server config at once; later setters override individual fields.
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

        /// Selects the server TLS mode (e.g. [Tls#custom] to serve TLS). Defaults to plaintext.
        public Builder setTls(Tls tls) {
            config.setTls(tls);
            return this;
        }

        /// Caps a single decoded export request; guards against memory-exhausting oversized
        /// requests. Defaults to 4 MiB.
        public Builder setMaxInboundMessageSizeBytes(int bytes) {
            config.setMaxInboundMessageSizeBytes(bytes);
            return this;
        }

        /// Caps in-flight calls per connection; `0` (default) is unlimited.
        public Builder setMaxConcurrentCallsPerConnection(int max) {
            config.setMaxConcurrentCallsPerConnection(max);
            return this;
        }

        /// Bounds the transport/TLS handshake deadline, not request body or connection idle time.
        /// Defaults to 20s.
        public Builder setHandshakeTimeout(Duration handshakeTimeout) {
            config.setHandshakeTimeout(handshakeTimeout);
            return this;
        }

        /// Supplies the executor that runs admitted calls. Defaults to gRPC's own executor.
        public Builder setServerExecutor(Executor serverExecutor) {
            config.setServerExecutor(serverExecutor);
            return this;
        }

        public Builder onTraces(TracesSink sink) {
            this.traces = sink;
            return this;
        }

        public Builder onMetrics(MetricsSink sink) {
            this.metrics = sink;
            return this;
        }

        public Builder onLogs(LogsSink sink) {
            this.logs = sink;
            return this;
        }

        public Builder onProfiles(ProfilesSink sink) {
            this.profiles = sink;
            return this;
        }

        public Receiver build() {
            return new ServerReceiver("OTLP/gRPC", disp -> new GrpcOtlpServer(config.build(), disp),
                    traces, metrics, logs, profiles);
        }
    }
}
