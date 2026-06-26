package dev.nthings.otlp4j.config;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Configuration for an OTLP receiver transport.
///
/// Beyond the bind address and [Tls], this carries the production-hardening limits the gRPC
/// receiver applies: a cap on decoded request size, an optional per-connection concurrency cap,
/// a deadline on the connection handshake, and an optional bounded server executor. The handshake
/// deadline bounds only the transport/TLS handshake; it does not bound a slow request body or an
/// idle connection post-handshake (keepalive / max-connection-idle limits are not yet exposed). The
/// defaults match gRPC's own, so an unconfigured receiver behaves as before but is now tunable.
///
/// Compression is asymmetric and intentionally one-sided. The server transparently DECODES gzip
/// request bodies via gRPC's default decoder, so no server-side switch is needed to accept
/// compressed exports. Response compression is deliberately not configured because OTLP export
/// responses are negligible (an empty ack or a small partial-success count). gzip on the request
/// path is selected by the client via [Compression]; there is no server compression knob.
public record ServerConfig(
        String bindHost,
        int port,
        Tls tls,
        int maxInboundMessageSizeBytes,
        int maxConcurrentCallsPerConnection,
        Duration handshakeTimeout,
        @Nullable Executor serverExecutor) {

    /// gRPC's own default decoded-message cap (4 MiB); used when the builder is left untouched.
    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES = 4 * 1024 * 1024;

    public ServerConfig {
        Objects.requireNonNull(bindHost, "bindHost");
        Objects.requireNonNull(tls, "tls");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (maxInboundMessageSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxInboundMessageSizeBytes must be positive: " + maxInboundMessageSizeBytes);
        }
        if (maxConcurrentCallsPerConnection < 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentCallsPerConnection must be >= 0: " + maxConcurrentCallsPerConnection);
        }
        Objects.requireNonNull(handshakeTimeout, "handshakeTimeout");
        if (handshakeTimeout.isZero() || handshakeTimeout.isNegative()) {
            throw new IllegalArgumentException("handshakeTimeout must be positive: " + handshakeTimeout);
        }
        // serverExecutor is nullable: null selects gRPC's default executor.
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a builder pre-populated with this config's fields.
    public Builder toBuilder() {
        return new Builder()
                .bindHost(bindHost)
                .port(port)
                .tls(tls)
                .maxInboundMessageSizeBytes(maxInboundMessageSizeBytes)
                .maxConcurrentCallsPerConnection(maxConcurrentCallsPerConnection)
                .handshakeTimeout(handshakeTimeout)
                .serverExecutor(serverExecutor);
    }

    public static final class Builder {

        private String bindHost = "0.0.0.0";
        private int port = 4317;
        private Tls tls = Tls.disabled();
        private int maxInboundMessageSizeBytes = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES;
        private int maxConcurrentCallsPerConnection = 0;
        private Duration handshakeTimeout = Duration.ofSeconds(20);
        private @Nullable Executor serverExecutor = null;

        private Builder() {}

        public Builder bindHost(String host) {
            this.bindHost = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder tls(Tls tls) {
            this.tls = tls;
            return this;
        }

        /// Caps the size of a single decoded export request (default 4 MiB); guards against
        /// memory-exhausting oversized requests.
        public Builder maxInboundMessageSizeBytes(int bytes) {
            this.maxInboundMessageSizeBytes = bytes;
            return this;
        }

        /// Caps concurrent in-flight calls per connection. `0` (the default) keeps gRPC's
        /// unlimited behaviour; a positive value is applied to the receiver.
        public Builder maxConcurrentCallsPerConnection(int max) {
            this.maxConcurrentCallsPerConnection = max;
            return this;
        }

        /// Bounds how long a new connection may take to finish its transport/TLS handshake
        /// (default 20s). Bounds the handshake only — not a slow request body or an idle connection.
        public Builder handshakeTimeout(Duration handshakeTimeout) {
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        /// Supplies the executor that runs the service handlers. `null` (the default) uses gRPC's
        /// own executor; pass a BOUNDED pool to cap the work the receiver will admit concurrently.
        public Builder serverExecutor(@Nullable Executor serverExecutor) {
            this.serverExecutor = serverExecutor;
            return this;
        }

        public ServerConfig build() {
            return new ServerConfig(
                    bindHost,
                    port,
                    tls,
                    maxInboundMessageSizeBytes,
                    maxConcurrentCallsPerConnection,
                    handshakeTimeout,
                    serverExecutor);
        }
    }
}
