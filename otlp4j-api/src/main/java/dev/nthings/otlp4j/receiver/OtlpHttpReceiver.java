package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.pipeline.LogConsumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.ProfileConsumer;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.spi.Protocol;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import dev.nthings.otlp4j.spi.Tls;
import java.time.Duration;
import java.util.concurrent.Executor;

/// Receives OTLP/HTTP requests (binary-protobuf bodies), dispatches them to per-signal consumers,
/// and exposes a [TelemetryTap] for live observation.
///
/// `.onTraces(...)`-style builder sugar attaches a single consumer per signal; richer graphs
/// (branches, fan-out) wire the [Source]s via `Pipeline.from(receiver.traces()) ...`. Shared
/// dispatch and lifecycle live in [AbstractOtlpReceiver]; this subclass selects the
/// [Protocol#HTTP_PROTOBUF] transport and exposes the HTTP-flavoured builder.
///
/// Exports are accepted by POST on the standard signal paths — `/v1/traces`, `/v1/metrics`,
/// `/v1/logs`, and `/v1development/profiles`. The default bind is `0.0.0.0:4318` with plaintext.
public final class OtlpHttpReceiver extends AbstractOtlpReceiver {

    /// The conventional OTLP/HTTP port, used as the builder default (gRPC uses 4317).
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpReceiver(Builder b) {
        super(b.config.build(), Protocol.HTTP_PROTOBUF, b.traces, b.metrics, b.logs, b.profiles);
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

        private ServerTransportConfig.Builder config =
                ServerTransportConfig.builder().port(DEFAULT_HTTP_PORT);
        private TraceConsumer    traces;
        private MetricConsumer   metrics;
        private LogConsumer      logs;
        private ProfileConsumer  profiles;

        private Builder() {}

        /// Replaces the whole transport config. The supplied config's port is used verbatim (the
        /// 4318 HTTP default applies only to the unconfigured builder).
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

        /// Caps in-flight calls per connection; `0` (the default) leaves it unlimited. Not all
        /// transports honour this; the HTTP receiver bounds concurrency through its server executor.
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

        /// Supplies the executor that runs admitted requests; a bounded pool caps concurrent work.
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

        public OtlpHttpReceiver build() {
            return new OtlpHttpReceiver(this);
        }
    }
}
