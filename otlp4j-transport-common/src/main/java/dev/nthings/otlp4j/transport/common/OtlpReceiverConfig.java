package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.component.ComponentConfig;
import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/// The neutral, transport-independent configuration for an OTLP receiver component. It carries the
/// bind address, TLS mode, and the hardening limits: the maximum decoded request size, the optional
/// global admission caps on in-flight requests and decoded bytes, and the optional throttle
/// retry-after hint. No third-party type appears on it. Both the gRPC and HTTP receiver factories
/// accept this one record; each supplies its own default port through [#defaults(int)]. The factory
/// translates it to the internal [ServerConfig] through [#toServerConfig()].
///
/// The gRPC-only per-connection concurrency cap is deliberately not modelled here, matching the
/// legacy [ServerConfig].
///
/// @param bindHost                   the bind host
/// @param port                       the bind port (`0` binds an ephemeral port)
/// @param tls                        the server TLS mode
/// @param maxInboundMessageSizeBytes the cap on a single decoded export request
/// @param maxInFlightRequests        the global in-flight request cap (`0` disables it)
/// @param maxInFlightBytes           the global in-flight decoded-byte cap (`0` disables it)
/// @param throttleRetryAfter         the hint attached to at-capacity rejections, may be null
public record OtlpReceiverConfig(
        String bindHost,
        int port,
        Tls tls,
        int maxInboundMessageSizeBytes,
        int maxInFlightRequests,
        long maxInFlightBytes,
        @Nullable Duration throttleRetryAfter)
        implements ComponentConfig {

    /// Validates the components.
    public OtlpReceiverConfig {
        Objects.requireNonNull(bindHost, "bindHost");
        Objects.requireNonNull(tls, "tls");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
    }

    /// The default receiver config for `port`: bound to `localhost:port`, plaintext, the shared 4 MiB
    /// decoded-request cap, no admission limits, and no throttle hint, matching the legacy
    /// [ServerConfig] defaults.
    ///
    /// @param port the default bind port for the transport (`4317` for gRPC, `4318` for HTTP)
    /// @return the default config
    public static OtlpReceiverConfig defaults(int port) {
        return new OtlpReceiverConfig(
                "localhost",
                port,
                Tls.disabled(),
                ServerConfig.DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES,
                0,
                0L,
                null);
    }

    /// A copy of this config bound to an ephemeral port (`0`); read the OS-assigned port back through
    /// the started receiver's [dev.nthings.otlp4j.core.component.NetworkBound] capability.
    ///
    /// @return an ephemeral-port copy
    public OtlpReceiverConfig withEphemeralPort() {
        return new OtlpReceiverConfig(
                bindHost, 0, tls, maxInboundMessageSizeBytes, maxInFlightRequests, maxInFlightBytes,
                throttleRetryAfter);
    }

    /// Translates this neutral config into the internal transport [ServerConfig], mapping every field
    /// across directly.
    ///
    /// @return the equivalent server config
    public ServerConfig toServerConfig() {
        var b = ServerConfig.builder()
                .setBindHost(bindHost)
                .setPort(port)
                .setTls(tls)
                .setMaxInboundMessageSizeBytes(maxInboundMessageSizeBytes)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setMaxInFlightBytes(maxInFlightBytes);
        if (throttleRetryAfter != null) {
            b.setThrottleRetryAfter(throttleRetryAfter);
        }
        return b.build();
    }
}
