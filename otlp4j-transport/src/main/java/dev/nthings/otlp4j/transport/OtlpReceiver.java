package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.internal.ArmeriaOtlpServer;
import dev.nthings.otlp4j.transport.internal.Protocol;
import java.time.Clock;
import java.time.Duration;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/// Builds [ServerReceiver]s that accept OTLP requests over Armeria and dispatch to the routed graph
/// consumers.
///
/// One server can serve OTLP/gRPC, OTLP/HTTP, or both from a single port. [#grpcBuilder()] serves
/// gRPC only (default bind `localhost:4317`), [#httpBuilder()] serves HTTP only (default bind
/// `localhost:4318`), and [#builder()] serves both. Call [ServerReceiver#start()] on the built
/// receiver to bind the transport.
///
/// Enforced client-certificate authentication (mutual TLS) is set through
/// [Builder#setClientAuth(ClientAuth)] and defaults to [ClientAuth#NONE].
public final class OtlpReceiver {

    private OtlpReceiver() {}

    /// How the server treats a client certificate during the TLS handshake. The trust anchor that
    /// verifies a presented client certificate is the CA supplied through the server's own
    /// [Tls.Custom] `trustFile` or [Tls.Inline] `trust`, which the server otherwise ignores.
    public enum ClientAuth {
        /// No client certificate is requested; a client presenting one still succeeds. The default.
        NONE,
        /// A client certificate is requested but not required: a certless client still completes the
        /// handshake, and a presented certificate is verified against the configured trust anchor.
        OPTIONAL,
        /// A client certificate is required: a certless client is rejected at the TLS handshake. This is
        /// the enforced mutual-TLS capability.
        REQUIRE
    }

    /// A builder serving OTLP/gRPC only, default bind `localhost:4317`.
    ///
    /// @return the gRPC receiver builder
    public static Builder grpcBuilder() {
        return new Builder(EnumSet.of(Protocol.GRPC), OtlpReceiverConfig.builder(4317), "OTLP/gRPC");
    }

    /// A builder serving OTLP/HTTP only, default bind `localhost:4318`.
    ///
    /// @return the HTTP receiver builder
    public static Builder httpBuilder() {
        return new Builder(EnumSet.of(Protocol.HTTP), OtlpReceiverConfig.builder(4318), "OTLP/HTTP");
    }

    /// A builder serving both OTLP/gRPC and OTLP/HTTP from one port, default bind `localhost:4317`.
    ///
    /// @return the dual-protocol receiver builder
    public static Builder builder() {
        return new Builder(EnumSet.of(Protocol.GRPC, Protocol.HTTP), OtlpReceiverConfig.builder(4317), "OTLP");
    }

    /// Builder for an OTLP [ServerReceiver]. The wire knobs are held on an [OtlpReceiverConfig.Builder]
    /// and build the same [OtlpReceiverConfig] the graph factory consumes; [#build()] wires the built
    /// config into an [ArmeriaOtlpServer] behind a [ServerReceiver] with the routed graph consumers
    /// attached.
    public static final class Builder {

        private final Set<Protocol> protocols;
        private OtlpReceiverConfig.Builder config;
        private final String transportName;
        private final Map<SignalDescriptor<?>, SignalConsumer<TelemetryPayload>> routes = new LinkedHashMap<>();
        private ComponentId id = ComponentId.parse("otlp");
        private Clock clock = Clock.systemUTC();
        private GatewayMetrics gatewayMetrics = GatewayMetrics.noop();
        private ClientAuth clientAuth = ClientAuth.NONE;

        private Builder(Set<Protocol> protocols, OtlpReceiverConfig.Builder config, String transportName) {
            this.protocols = protocols;
            this.config = Objects.requireNonNull(config, "config");
            this.transportName = Objects.requireNonNull(transportName, "transportName");
        }

        /// Replaces the whole server config at once; later setters override individual fields.
        ///
        /// @param config the replacement config
        /// @return this builder
        public Builder setConfig(OtlpReceiverConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        /// Sets the bind host and port.
        ///
        /// @param bindHost the bind host
        /// @param port     the bind port
        /// @return this builder
        public Builder setEndpoint(String bindHost, int port) {
            config.setBindHost(bindHost).setPort(port);
            return this;
        }

        /// Sets the bind port.
        ///
        /// @param port the bind port
        /// @return this builder
        public Builder setPort(int port) {
            config.setPort(port);
            return this;
        }

        /// Binds an ephemeral port (port `0`); read the actual port from the built [ServerReceiver]'s
        /// `port()`.
        ///
        /// @return this builder
        public Builder ephemeralPort() {
            return setPort(0);
        }

        /// Selects the server TLS mode (e.g. [Tls#custom] to serve TLS). Defaults to plaintext.
        ///
        /// @param tls the TLS mode
        /// @return this builder
        public Builder setTls(Tls tls) {
            config.setTls(tls);
            return this;
        }

        /// Caps a single decoded export request; guards against memory-exhausting oversized requests.
        /// Defaults to 4 MiB.
        ///
        /// @param bytes the per-request cap in bytes
        /// @return this builder
        public Builder setMaxInboundMessageSizeBytes(int bytes) {
            config.setMaxInboundMessageSizeBytes(bytes);
            return this;
        }

        /// Caps decoded export requests in flight across every connection; excess requests are rejected
        /// retryably ("at capacity"). `0` (the default) disables the cap.
        ///
        /// @param max the global in-flight request cap
        /// @return this builder
        public Builder setMaxInFlightRequests(int max) {
            config.setMaxInFlightRequests(max);
            return this;
        }

        /// Caps decoded request bytes in flight across every connection; excess requests are rejected
        /// retryably ("at capacity"). A request is always admitted when nothing else is in flight. `0`
        /// (the default) disables the cap.
        ///
        /// @param bytes the global in-flight byte cap
        /// @return this builder
        public Builder setMaxInFlightBytes(long bytes) {
            config.setMaxInFlightBytes(bytes);
            return this;
        }

        /// Sets the retry-after hint attached to admission rejections (gRPC pushback trailer / HTTP
        /// `Retry-After`). Unset (the default) emits no hint.
        ///
        /// @param throttleRetryAfter the hint attached to "at capacity" rejections
        /// @return this builder
        public Builder setThrottleRetryAfter(Duration throttleRetryAfter) {
            config.setThrottleRetryAfter(throttleRetryAfter);
            return this;
        }

        /// Sets how the server treats a client certificate during the TLS handshake. Under
        /// [ClientAuth#REQUIRE] a certless client is rejected at the handshake (enforced mutual TLS);
        /// under [ClientAuth#OPTIONAL] a presented certificate is verified but a certless client still
        /// succeeds; under the default [ClientAuth#NONE] no client certificate is requested. Requesting
        /// or requiring a client certificate needs a trust anchor, supplied through the server TLS mode
        /// ([Tls.Custom] `trustFile` or [Tls.Inline] `trust`); an enforced or optional mode without one
        /// fails at start.
        ///
        /// @param clientAuth the client-authentication mode
        /// @return this builder
        public Builder setClientAuth(ClientAuth clientAuth) {
            this.clientAuth = Objects.requireNonNull(clientAuth, "clientAuth");
            return this;
        }

        /// Routes `consumer` as the graph consumer for `signal`. A second route for the same signal
        /// replaces the first.
        ///
        /// @param signal   the signal to route
        /// @param consumer the graph consumer for that signal
        /// @return this builder
        public Builder route(SignalDescriptor<?> signal, SignalConsumer<TelemetryPayload> consumer) {
            routes.put(Objects.requireNonNull(signal, "signal"), Objects.requireNonNull(consumer, "consumer"));
            return this;
        }

        /// Sets the receiver component identity, attributed to no-consumer rejections. Defaults to the
        /// `otlp` type with no name.
        ///
        /// @param id the receiver component identity
        /// @return this builder
        public Builder componentId(ComponentId id) {
            this.id = Objects.requireNonNull(id, "id");
            return this;
        }

        /// Sets the clock that stamps each consume deadline. Defaults to the system UTC clock.
        ///
        /// @param clock the clock
        /// @return this builder
        public Builder clock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /// Sets the [GatewayMetrics] registry the receiver reports to (default [GatewayMetrics#noop]).
        ///
        /// @param telemetry the self-observability registry
        /// @return this builder
        public Builder telemetry(GatewayMetrics telemetry) {
            this.gatewayMetrics = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        /// Builds the receiver: finishes the config, opens the wire server, and wraps it in a
        /// [ServerReceiver] with the routed graph consumers attached.
        ///
        /// @return the receiver; call [ServerReceiver#start()] to bind the transport
        public ServerReceiver build() {
            var built = config.build();
            return new ServerReceiver(transportName, id, clock, gatewayMetrics,
                    disp -> new ArmeriaOtlpServer(built, disp, gatewayMetrics, protocols, clientAuth), routes);
        }
    }
}
