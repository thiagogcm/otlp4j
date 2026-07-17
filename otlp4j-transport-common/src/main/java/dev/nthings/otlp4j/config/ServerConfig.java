package dev.nthings.otlp4j.config;

import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/// Configuration for an OTLP receiver transport.
///
/// Carries bind address, [Tls], and hardening limits: max decoded request size, optional global
/// admission limits (in-flight requests and decoded bytes across every connection, with an
/// optional throttle retry-after hint). Defaults match gRPC's own. The gRPC-only per-connection
/// concurrency cap is handled by the transport's own runtime, not a shared config field (HTTP has
/// no equivalent).
///
/// Compression is asymmetric: the server transparently decodes gzip requests but response
/// compression is not configured (OTLP responses are negligible). gzip is selected by the
/// client via [Compression].
public record ServerConfig(
        String bindHost,
        int port,
        Tls tls,
        int maxInboundMessageSizeBytes,
        int maxInFlightRequests,
        long maxInFlightBytes,
        @Nullable Duration throttleRetryAfter) {

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
        if (maxInFlightRequests < 0) {
            throw new IllegalArgumentException("maxInFlightRequests must be >= 0: " + maxInFlightRequests);
        }
        if (maxInFlightBytes < 0) {
            throw new IllegalArgumentException("maxInFlightBytes must be >= 0: " + maxInFlightBytes);
        }
        if (throttleRetryAfter != null && (throttleRetryAfter.isZero() || throttleRetryAfter.isNegative())) {
            throw new IllegalArgumentException("throttleRetryAfter must be positive: " + throttleRetryAfter);
        }
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
        var builder = new Builder()
                .setBindHost(bindHost)
                .setPort(port)
                .setTls(tls)
                .setMaxInboundMessageSizeBytes(maxInboundMessageSizeBytes)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setMaxInFlightBytes(maxInFlightBytes);
        if (throttleRetryAfter != null) {
            builder.setThrottleRetryAfter(throttleRetryAfter);
        }
        return builder;
    }

    public static final class Builder {

        private String bindHost = "localhost";
        private int port = 4317;
        private Tls tls = Tls.disabled();
        private int maxInboundMessageSizeBytes = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES;
        private int maxInFlightRequests = 0;
        private long maxInFlightBytes = 0L;
        private @Nullable Duration throttleRetryAfter = null;

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

        /// Caps decoded export requests in flight across every connection; the receiver rejects
        /// excess requests retryably ("at capacity"). `0` (the default) disables the cap.
        public Builder setMaxInFlightRequests(int max) {
            this.maxInFlightRequests = max;
            return this;
        }

        /// Caps decoded request bytes in flight across every connection; the receiver rejects
        /// excess requests retryably ("at capacity"). A single request is always admitted when
        /// nothing else is in flight, so an oversized request cannot starve. `0` (the default)
        /// disables the cap.
        public Builder setMaxInFlightBytes(long bytes) {
            this.maxInFlightBytes = bytes;
            return this;
        }

        /// Sets the retry-after hint attached to admission rejections (gRPC pushback trailer /
        /// HTTP `Retry-After`), telling throttled senders how long to wait. Unset (the default)
        /// emits no hint.
        public Builder setThrottleRetryAfter(Duration throttleRetryAfter) {
            this.throttleRetryAfter = throttleRetryAfter;
            return this;
        }

        public ServerConfig build() {
            return new ServerConfig(
                    bindHost,
                    port,
                    tls,
                    maxInboundMessageSizeBytes,
                    maxInFlightRequests,
                    maxInFlightBytes,
                    throttleRetryAfter);
        }
    }
}
