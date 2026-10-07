package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/// The configuration for an OTLP receiver. It is the single source of truth the transport is built
/// from: the bind address, TLS mode, and the hardening limits: the maximum decoded request size, the
/// longest deadline a gRPC client may request, the optional global admission caps on in-flight
/// requests and decoded bytes, and the optional throttle retry-after hint. No third-party type appears on it. Both the gRPC and HTTP receiver factories
/// accept this one record; each supplies its own default port through [#defaults(int)]. The wire
/// server ([dev.nthings.otlp4j.transport.internal.ArmeriaOtlpServer]) consumes it directly.
///
/// The gRPC-only per-connection concurrency cap is deliberately not modelled here; the transport's own
/// runtime handles it (HTTP has no equivalent). Compression is asymmetric: the server transparently
/// decodes gzip requests but response compression is not configured (OTLP responses are negligible);
/// gzip is selected by the client via [Compression].
///
/// @param bindHost                   the bind host
/// @param port                       the bind port (`0` binds an ephemeral port)
/// @param tls                        the server TLS mode
/// @param clientAuth                 how the server treats a client certificate (enforced mutual TLS)
/// @param maxInboundMessageSizeBytes the cap on a single decoded export request
/// @param maxClientTimeout           the longest deadline a gRPC client may request through `grpc-timeout`
/// @param maxInFlightRequests        the global in-flight request cap (`0` disables it)
/// @param maxInFlightBytes           the global in-flight decoded-byte cap (`0` disables it)
/// @param throttleRetryAfter         the hint attached to at-capacity rejections, may be null
public record OtlpReceiverConfig(
        String bindHost,
        int port,
        Tls tls,
        ClientAuth clientAuth,
        int maxInboundMessageSizeBytes,
        Duration maxClientTimeout,
        int maxInFlightRequests,
        long maxInFlightBytes,
        @Nullable Duration throttleRetryAfter)
        implements ComponentConfig {

    /// Default decoded request cap (64 MiB); used when the builder is left untouched.
    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES = 64 * 1024 * 1024;

    /// Default cap on a client-requested gRPC deadline (30 seconds); used when the builder is left
    /// untouched. It is also the budget granted when no request timeout is known.
    public static final Duration DEFAULT_MAX_CLIENT_TIMEOUT = Duration.ofSeconds(30);

    /// Validates the components and the hardening limits.
    public OtlpReceiverConfig {
        Objects.requireNonNull(bindHost, "bindHost");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(clientAuth, "clientAuth");
        Objects.requireNonNull(maxClientTimeout, "maxClientTimeout");
        Ports.requireValid(port);
        if (maxInboundMessageSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxInboundMessageSizeBytes must be positive: " + maxInboundMessageSizeBytes);
        }
        if (maxClientTimeout.isZero() || maxClientTimeout.isNegative()) {
            throw new IllegalArgumentException("maxClientTimeout must be positive: " + maxClientTimeout);
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

    /// The default receiver config for `port`: bound to `localhost:port`, plaintext, the shared 64 MiB
    /// decoded-request cap, the 30 second client-timeout cap, no admission limits, and no throttle
    /// hint.
    ///
    /// @param port the default bind port for the transport (`4317` for gRPC, `4318` for HTTP)
    /// @return the default config
    public static OtlpReceiverConfig defaults(int port) {
        return new OtlpReceiverConfig(
                "localhost", port, Tls.disabled(), ClientAuth.NONE, DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES,
                DEFAULT_MAX_CLIENT_TIMEOUT, 0, 0L, null);
    }

    /// A builder seeded with `port` as the bind port and all other builder defaults.
    ///
    /// @param port the bind port
    /// @return a fresh builder
    public static Builder builder(int port) {
        return new Builder(port);
    }

    /// A builder seeded with this config's fields.
    ///
    /// @return a builder pre-populated from this config
    public Builder toBuilder() {
        var b = new Builder(port)
                .bindHost(bindHost)
                .tls(tls)
                .clientAuth(clientAuth)
                .maxInboundMessageSizeBytes(maxInboundMessageSizeBytes)
                .maxClientTimeout(maxClientTimeout)
                .maxInFlightRequests(maxInFlightRequests)
                .maxInFlightBytes(maxInFlightBytes);
        if (throttleRetryAfter != null) {
            b.throttleRetryAfter(throttleRetryAfter);
        }
        return b;
    }

    /// A copy of this config bound to an ephemeral port (`0`); read the OS-assigned port from the
    /// runtime after startup.
    ///
    /// @return an ephemeral-port copy
    public OtlpReceiverConfig withEphemeralPort() {
        return new OtlpReceiverConfig(
                bindHost, 0, tls, clientAuth, maxInboundMessageSizeBytes, maxClientTimeout,
                maxInFlightRequests, maxInFlightBytes, throttleRetryAfter);
    }

    /// A fluent builder for [OtlpReceiverConfig].
    public static final class Builder {

        private String bindHost = "localhost";
        private int port;
        private Tls tls = Tls.disabled();
        private ClientAuth clientAuth = ClientAuth.NONE;
        private int maxInboundMessageSizeBytes = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES;
        private Duration maxClientTimeout = DEFAULT_MAX_CLIENT_TIMEOUT;
        private int maxInFlightRequests = 0;
        private long maxInFlightBytes = 0L;
        private @Nullable Duration throttleRetryAfter = null;

        private Builder(int port) {
            this.port = port;
        }

        /// Sets the bind host.
        ///
        /// @param host the bind host
        /// @return this builder
        public Builder bindHost(String host) {
            this.bindHost = host;
            return this;
        }

        /// Sets the bind port.
        ///
        /// @param port the bind port
        /// @return this builder
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /// Selects the server TLS mode.
        ///
        /// @param tls the TLS mode
        /// @return this builder
        public Builder tls(Tls tls) {
            this.tls = tls;
            return this;
        }

        /// Sets how the server treats a client certificate during the TLS handshake (enforced mutual
        /// TLS). Under [ClientAuth#REQUIRE] a certless client is rejected at the handshake; under
        /// [ClientAuth#OPTIONAL] a presented certificate is verified but a certless client still
        /// succeeds; under the default [ClientAuth#NONE] no client certificate is requested. Requesting
        /// or requiring a client certificate needs a trust anchor, supplied through the server TLS mode
        /// ([Tls.Custom] `trustFile` or [Tls.Inline] `trust`); its absence fails at start.
        ///
        /// @param clientAuth the client-authentication mode
        /// @return this builder
        public Builder clientAuth(ClientAuth clientAuth) {
            this.clientAuth = Objects.requireNonNull(clientAuth, "clientAuth");
            return this;
        }

        /// Caps the size of a single decoded export request (default 64 MiB); guards against
        /// memory-exhausting oversized requests.
        ///
        /// @param bytes the per-request cap in bytes
        /// @return this builder
        public Builder maxInboundMessageSizeBytes(int bytes) {
            this.maxInboundMessageSizeBytes = bytes;
            return this;
        }

        /// Caps the deadline a gRPC client may request through the `grpc-timeout` header (default 30
        /// seconds). A longer deadline, or none at all, is replaced by this value, so an untrusted client
        /// cannot hold a request open indefinitely; a shorter one is honoured as sent. OTLP/HTTP clients
        /// cannot choose a deadline, so the cap does not apply to them.
        ///
        /// @param timeout the longest client-requested deadline, which must be positive
        /// @return this builder
        public Builder maxClientTimeout(Duration timeout) {
            this.maxClientTimeout = timeout;
            return this;
        }

        /// Caps decoded export requests in flight across every connection; excess requests are rejected
        /// retryably ("at capacity"). `0` (the default) disables the cap.
        ///
        /// @param max the global in-flight request cap
        /// @return this builder
        public Builder maxInFlightRequests(int max) {
            this.maxInFlightRequests = max;
            return this;
        }

        /// Caps decoded request bytes in flight across every connection; excess requests are rejected
        /// retryably ("at capacity"). A single request is always admitted when nothing else is in
        /// flight. `0` (the default) disables the cap.
        ///
        /// @param bytes the global in-flight byte cap
        /// @return this builder
        public Builder maxInFlightBytes(long bytes) {
            this.maxInFlightBytes = bytes;
            return this;
        }

        /// Sets the retry-after hint attached to admission rejections (gRPC pushback trailer / HTTP
        /// `Retry-After`), telling throttled senders how long to wait. Unset (the default) emits no
        /// hint.
        ///
        /// @param throttleRetryAfter the hint attached to "at capacity" rejections
        /// @return this builder
        public Builder throttleRetryAfter(Duration throttleRetryAfter) {
            this.throttleRetryAfter = throttleRetryAfter;
            return this;
        }

        /// Builds the config.
        ///
        /// @return the config
        public OtlpReceiverConfig build() {
            return new OtlpReceiverConfig(
                    bindHost, port, tls, clientAuth, maxInboundMessageSizeBytes, maxClientTimeout,
                    maxInFlightRequests, maxInFlightBytes, throttleRetryAfter);
        }
    }
}
