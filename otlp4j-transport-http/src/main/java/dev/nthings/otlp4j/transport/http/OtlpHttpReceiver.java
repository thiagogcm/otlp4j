package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.receiver.AbstractOtlpReceiver;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpServer;
import java.time.Duration;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Receives OTLP/HTTP requests (binary-protobuf bodies), dispatches them to per-signal sinks,
/// and exposes a telemetry tap for live observation.
///
/// `.onTraces(...)`-style builder sugar attaches a single sink per signal; richer graphs
/// (branches, fan-out) wire the sources via `Pipeline.from(receiver.traces()) ...`. Shared
/// dispatch and lifecycle live in [AbstractOtlpReceiver].
///
/// Exports are accepted by POST on the standard signal paths — `/v1/traces`, `/v1/metrics`,
/// `/v1/logs`, and `/v1development/profiles`. The default bind is `0.0.0.0:4318` with plaintext.
public final class OtlpHttpReceiver extends AbstractOtlpReceiver {

    /// The conventional OTLP/HTTP port, used as the builder default (gRPC uses 4317).
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpReceiver(Builder b) {
        super("OTLP/HTTP", disp -> new HttpOtlpServer(b.config.build(), disp),
                b.traces, b.metrics, b.logs, b.profiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Builds — but does not [#start()] — a receiver bound to `port` on all interfaces.
    public static OtlpHttpReceiver on(int port) {
        return builder().port(port).build();
    }

    /// Builds — but does not [#start()] — a receiver bound to `bindHost:port`.
    public static OtlpHttpReceiver on(String bindHost, int port) {
        return builder().endpoint(bindHost, port).build();
    }

    @Override
    public OtlpHttpReceiver start() {
        startServer();
        return this;
    }

    /// Builder for [OtlpHttpReceiver]. Defaults bind `0.0.0.0:4318` with plaintext transport.
    public static final class Builder {

        private ServerConfig.Builder config =
                ServerConfig.builder().port(DEFAULT_HTTP_PORT);
        private @Nullable TraceSink    traces;
        private @Nullable MetricSink   metrics;
        private @Nullable LogSink      logs;
        private @Nullable ProfileSink  profiles;

        private Builder() {}

        /// Replaces the whole transport config. The supplied config's port is used verbatim (the
        /// 4318 HTTP default applies only to the unconfigured builder).
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

        /// Bounds the transport/TLS handshake only — not a slow request body or idle connection.
        /// Defaults to 20s.
        public Builder handshakeTimeout(Duration handshakeTimeout) {
            config.handshakeTimeout(handshakeTimeout);
            return this;
        }

        /// Supplies the executor that runs admitted requests; a bounded pool caps concurrent work.
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

        public OtlpHttpReceiver build() {
            return new OtlpHttpReceiver(this);
        }
    }
}
