package dev.nthings.otlp4j.config;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Configuration for an OTLP receiver transport.
///
/// Carries bind address, [Tls], and hardening limits: max decoded request size, optional
/// per-connection concurrency cap, handshake deadline, and optional server executor. Defaults
/// match gRPC's own. The handshake deadline bounds only the transport/TLS handshake - not a slow
/// request body or idle connection.
///
/// Compression is asymmetric: the server transparently decodes gzip requests but response
/// compression is not configured (OTLP responses are negligible). gzip is selected by the
/// client via [Compression].
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

    /// The default config (all builder defaults, `localhost:4317` plaintext).
    public static ServerConfig defaults() {
        return builder().build();
    }

    /// Returns a builder pre-populated with this config's fields.
    public Builder toBuilder() {
        return new Builder()
                .setBindHost(bindHost)
                .setPort(port)
                .setTls(tls)
                .setMaxInboundMessageSizeBytes(maxInboundMessageSizeBytes)
                .setMaxConcurrentCallsPerConnection(maxConcurrentCallsPerConnection)
                .setHandshakeTimeout(handshakeTimeout)
                .setServerExecutor(serverExecutor);
    }

    public static final class Builder {

        private String bindHost = "localhost";
        private int port = 4317;
        private Tls tls = Tls.disabled();
        private int maxInboundMessageSizeBytes = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES;
        private int maxConcurrentCallsPerConnection = 0;
        private Duration handshakeTimeout = Duration.ofSeconds(20);
        private @Nullable Executor serverExecutor = null;

        private Builder() {}

        public Builder setBindHost(String host) {
            this.bindHost = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTls(Tls tls) {
            this.tls = tls;
            return this;
        }

        /// Caps the size of a single decoded export request (default 4 MiB); guards against
        /// memory-exhausting oversized requests.
        public Builder setMaxInboundMessageSizeBytes(int bytes) {
            this.maxInboundMessageSizeBytes = bytes;
            return this;
        }

        /// Caps concurrent in-flight calls per connection. `0` (the default) keeps gRPC's
        /// unlimited behaviour; a positive value is applied to the receiver.
        public Builder setMaxConcurrentCallsPerConnection(int max) {
            this.maxConcurrentCallsPerConnection = max;
            return this;
        }

        /// Bounds how long a new connection may take to finish its transport/TLS handshake
        /// (default 20s). Handshake-only bound, not a slow body or idle connection timeout.
        public Builder setHandshakeTimeout(Duration handshakeTimeout) {
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        /// Supplies the executor that runs the service handlers. `null` (the default) selects the
        /// transport's default; pass a BOUNDED pool to cap the work the receiver will admit
        /// concurrently.
        public Builder setServerExecutor(@Nullable Executor serverExecutor) {
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
