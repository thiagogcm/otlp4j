package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.receiver.AbstractOtlpReceiver;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpServer;
import java.time.Duration;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Receives OTLP/gRPC requests, dispatches them to per-signal sinks, and exposes a telemetry tap
/// for live observation.
///
/// `.onTraces(...)`-style builder sugar attaches a single sink per signal; richer graphs
/// (branches, fan-out) wire the sources via `Pipeline.from(receiver.traces()) ...`. Shared
/// dispatch and lifecycle live in [AbstractOtlpReceiver]. The default bind is `0.0.0.0:4317`.
public final class OtlpGrpcReceiver extends AbstractOtlpReceiver {

    private OtlpGrpcReceiver(Builder b) {
        super("OTLP/gRPC", disp -> new GrpcOtlpServer(b.config.build(), disp),
                b.traces, b.metrics, b.logs, b.profiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Builds (but does not [#start()]) a receiver bound to `port` on all interfaces.
    public static OtlpGrpcReceiver on(int port) {
        return builder().port(port).build();
    }

    /// Builds — but does not [#start()] — a receiver bound to `bindHost:port`.
    public static OtlpGrpcReceiver on(String bindHost, int port) {
        return builder().endpoint(bindHost, port).build();
    }

    @Override
    public OtlpGrpcReceiver start() {
        startServer();
        return this;
    }

    /// Builder for [OtlpGrpcReceiver]. Defaults bind `0.0.0.0:4317` with plaintext transport.
    public static final class Builder {

        private ServerConfig.Builder config = ServerConfig.builder();
        private @Nullable TraceSink    traces;
        private @Nullable MetricSink   metrics;
        private @Nullable LogSink      logs;
        private @Nullable ProfileSink  profiles;

        private Builder() {}

        public Builder transport(ServerConfig config) {
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

        public OtlpGrpcReceiver build() {
            return new OtlpGrpcReceiver(this);
        }
    }
}
