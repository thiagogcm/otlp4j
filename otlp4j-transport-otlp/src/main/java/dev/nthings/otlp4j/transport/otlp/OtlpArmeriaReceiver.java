package dev.nthings.otlp4j.transport.otlp;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.common.AbstractReceiverBuilder;
import dev.nthings.otlp4j.transport.common.ServerReceiver;
import dev.nthings.otlp4j.transport.otlp.internal.ArmeriaOtlpServer;
import dev.nthings.otlp4j.transport.otlp.internal.Protocol;
import java.util.EnumSet;
import java.util.Set;

/// Builds [ServerReceiver]s that accept OTLP requests over Armeria and dispatch to the routed graph
/// consumers.
///
/// One server can serve OTLP/gRPC, OTLP/HTTP, or both from a single port. [#grpcBuilder()] serves
/// gRPC only (default bind `localhost:4317`), [#httpBuilder()] serves HTTP only (default bind
/// `localhost:4318`), and [#builder()] serves both. Call [ServerReceiver#start()] on the built
/// receiver to bind the transport.
///
/// Enforced client-certificate authentication (mutual TLS) is a transport-specific capability set
/// through [Builder#setClientAuth(ClientAuth)]. It is an Armeria-builder knob only: it does not touch
/// the neutral config records, so the shared conformance suites and every existing construction site
/// are unchanged, and it defaults to [ClientAuth#NONE].
public final class OtlpArmeriaReceiver {

    private OtlpArmeriaReceiver() {}

    /// How the server treats a client certificate during the TLS handshake. The trust anchor that
    /// verifies a presented client certificate is the CA supplied through the server's own
    /// [dev.nthings.otlp4j.config.Tls.Custom] `trustFile` or [dev.nthings.otlp4j.config.Tls.Inline]
    /// `trust`, which the server otherwise ignores.
    public enum ClientAuth {
        /// No client certificate is requested; a client presenting one still succeeds. The default, and
        /// the behaviour of the legacy transports, so the pinned
        /// mutual-TLS-accepted-but-not-required conformance case stays green.
        NONE,
        /// A client certificate is requested but not required: a certless client still completes the
        /// handshake, and a presented certificate is verified against the configured trust anchor.
        OPTIONAL,
        /// A client certificate is required: a certless client is rejected at the TLS handshake. This is
        /// the enforced mutual-TLS capability the legacy transports could not express.
        REQUIRE
    }

    /// A builder serving OTLP/gRPC only, default bind `localhost:4317`.
    ///
    /// @return the gRPC receiver builder
    public static Builder grpcBuilder() {
        return new Builder(EnumSet.of(Protocol.GRPC), ServerConfig.builder(), "OTLP/gRPC");
    }

    /// A builder serving OTLP/HTTP only, default bind `localhost:4318`.
    ///
    /// @return the HTTP receiver builder
    public static Builder httpBuilder() {
        return new Builder(EnumSet.of(Protocol.HTTP), ServerConfig.builder().setPort(4318), "OTLP/HTTP");
    }

    /// A builder serving both OTLP/gRPC and OTLP/HTTP from one port, default bind `localhost:4317`.
    ///
    /// @return the dual-protocol receiver builder
    public static Builder builder() {
        return new Builder(EnumSet.of(Protocol.GRPC, Protocol.HTTP), ServerConfig.builder(), "OTLP");
    }

    /// Builder for an Armeria OTLP [ServerReceiver]; the shared knobs live on
    /// [AbstractReceiverBuilder].
    public static final class Builder extends AbstractReceiverBuilder<Builder> {

        private final Set<Protocol> protocols;
        private ClientAuth clientAuth = ClientAuth.NONE;

        private Builder(Set<Protocol> protocols, ServerConfig.Builder config, String transportName) {
            super(config, transportName);
            this.protocols = protocols;
        }

        /// Sets how the server treats a client certificate during the TLS handshake. Under
        /// [ClientAuth#REQUIRE] a certless client is rejected at the handshake (enforced mutual TLS);
        /// under [ClientAuth#OPTIONAL] a presented certificate is verified but a certless client still
        /// succeeds; under the default [ClientAuth#NONE] no client certificate is requested. Requesting
        /// or requiring a client certificate needs a trust anchor, supplied through the server TLS mode
        /// ([dev.nthings.otlp4j.config.Tls.Custom] `trustFile` or [dev.nthings.otlp4j.config.Tls.Inline]
        /// `trust`); an enforced or optional mode without one fails at [#build()]/start.
        ///
        /// @param clientAuth the client-authentication mode
        /// @return this builder
        public Builder setClientAuth(ClientAuth clientAuth) {
            this.clientAuth = clientAuth;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected OtlpServer newServer(
                ServerConfig config, Dispatchers dispatchers, GatewayMetrics telemetry) {
            return new ArmeriaOtlpServer(config, dispatchers, telemetry, protocols, clientAuth);
        }
    }
}
