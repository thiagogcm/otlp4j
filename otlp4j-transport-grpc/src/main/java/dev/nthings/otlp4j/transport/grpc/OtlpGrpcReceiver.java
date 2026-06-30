package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
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

    /// Builds (but does not start) a receiver bound to `port` on the default loopback host.
    public static Receiver on(int port) {
        return builder().port(port).build();
    }

    /// Builds (but does not start) a receiver bound to `bindHost:port`.
    public static Receiver on(String bindHost, int port) {
        return builder().endpoint(bindHost, port).build();
    }

    /// Builder for an OTLP/gRPC [Receiver]. Defaults bind `localhost:4317` with plaintext transport.
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

        /// Caps in-flight calls per connection; `0` (default) is unlimited.
        public Builder maxConcurrentCallsPerConnection(int max) {
            config.maxConcurrentCallsPerConnection(max);
            return this;
        }

        /// Bounds the transport/TLS handshake deadline, not request body or connection idle time.
        /// Defaults to 20s.
        public Builder handshakeTimeout(Duration handshakeTimeout) {
            config.handshakeTimeout(handshakeTimeout);
            return this;
        }

        /// Supplies the executor that runs admitted calls. Defaults to gRPC's own executor.
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

        public Receiver build() {
            return new ServerReceiver("OTLP/gRPC", disp -> new GrpcOtlpServer(config.build(), disp),
                    traces, metrics, logs, profiles);
        }
    }
}
