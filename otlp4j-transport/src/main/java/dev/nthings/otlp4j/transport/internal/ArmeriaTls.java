package dev.nthings.otlp4j.transport.internal;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.server.ServerBuilder;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.transport.OtlpReceiver.ClientAuth;
import java.io.ByteArrayInputStream;
import org.jspecify.annotations.Nullable;

/// Maps the neutral [Tls] sealed family onto Armeria's server and client TLS surfaces.
///
/// Server side: [Tls.Custom] and [Tls.Inline] supply the certificate and key through
/// `ServerBuilder.tls(...)`; [Tls.SystemTrust] and [Tls.SslContext] are rejected because a server has
/// no certificate of its own, matching the legacy transports. When the receiver requests or requires a
/// client certificate ([ClientAuth#OPTIONAL] or [ClientAuth#REQUIRE]), a `tlsCustomizer` sets the Netty
/// client-auth mode and the trust anchor drawn from the server TLS mode's own CA material, so a
/// certless client is rejected at the handshake under [ClientAuth#REQUIRE]. Client side: every variant
/// maps onto a [ClientFactory] with a Netty `tlsCustomizer`, including the mutual-TLS key manager for
/// [Tls.Custom] and [Tls.Inline] client certificates; a null factory selects Armeria's default
/// (plaintext for [Tls.Disabled], system-trust HTTPS for [Tls.SystemTrust]).
final class ArmeriaTls {

    private ArmeriaTls() {}

    /// The URL scheme a client should use for `tls`: `http` for plaintext, `https` otherwise.
    ///
    /// @param tls the client TLS mode
    /// @return `http` or `https`
    static String clientScheme(Tls tls) {
        return tls instanceof Tls.Disabled ? "http" : "https";
    }

    /// Applies server TLS to `sb` from `tls`, with the client-authentication mode `clientAuth`. Only
    /// [Tls.Custom] and [Tls.Inline] are valid on a server; the other variants throw
    /// [IllegalArgumentException]. An [ClientAuth#OPTIONAL] or [ClientAuth#REQUIRE] mode needs the CA
    /// trust anchor supplied through the TLS mode ([Tls.Custom] `trustFile` or [Tls.Inline] `trust`);
    /// its absence throws.
    ///
    /// @param sb         the server builder
    /// @param tls        the server TLS mode
    /// @param clientAuth the client-certificate authentication mode
    static void applyServer(ServerBuilder sb, Tls tls, ClientAuth clientAuth) {
        switch (tls) {
            case Tls.Custom(var certFile, var keyFile, var trustFile) -> {
                if (certFile == null || keyFile == null) {
                    throw new IllegalArgumentException(
                            "Tls.Custom for a server requires both a certificate and a key");
                }
                sb.tls(certFile.toFile(), keyFile.toFile());
                if (clientAuth != ClientAuth.NONE) {
                    if (trustFile == null) {
                        throw new IllegalArgumentException(
                                "client authentication requires a CA trust anchor; set Tls.Custom trustFile");
                    }
                    var ca = trustFile.toFile();
                    var mode = nettyClientAuth(clientAuth);
                    sb.tlsCustomizer(b -> b.clientAuth(mode).trustManager(ca));
                }
            }
            case Tls.Inline(var cert, var key, var trust) -> {
                if (cert == null || key == null) {
                    throw new IllegalArgumentException(
                            "in-memory server TLS requires both a certificate and a key");
                }
                sb.tls(new ByteArrayInputStream(cert), new ByteArrayInputStream(key));
                if (clientAuth != ClientAuth.NONE) {
                    if (trust == null) {
                        throw new IllegalArgumentException(
                                "client authentication requires a CA trust anchor; set Tls.Inline trust");
                    }
                    var mode = nettyClientAuth(clientAuth);
                    // A fresh stream per build: the customizer may run more than once.
                    sb.tlsCustomizer(b -> b.clientAuth(mode).trustManager(new ByteArrayInputStream(trust)));
                }
            }
            case Tls.Disabled ignored -> throw new IllegalArgumentException(
                    "Tls.Disabled is not a TLS mode; the server binds plaintext instead");
            case Tls.SystemTrust ignored -> throw new IllegalArgumentException(
                    "Tls.SystemTrust is not valid for a server; use Tls.Custom with a certificate and key");
            case Tls.SslContext ignored -> throw new IllegalArgumentException(
                    "Tls.SslContext is not valid for a server; use Tls.Custom with a certificate and key");
        }
    }

    /// Maps the neutral [ClientAuth] mode onto the Netty client-auth mode.
    private static io.netty.handler.ssl.ClientAuth nettyClientAuth(ClientAuth mode) {
        return switch (mode) {
            case NONE -> io.netty.handler.ssl.ClientAuth.NONE;
            case OPTIONAL -> io.netty.handler.ssl.ClientAuth.OPTIONAL;
            case REQUIRE -> io.netty.handler.ssl.ClientAuth.REQUIRE;
        };
    }

    /// Builds the [ClientFactory] for `tls`, or null to use Armeria's default factory ([Tls.Disabled]
    /// plaintext and [Tls.SystemTrust] system-trust HTTPS both need no customization).
    ///
    /// @param tls the client TLS mode
    /// @return the client factory, or null for the default
    static @Nullable ClientFactory clientFactory(Tls tls) {
        return switch (tls) {
            case Tls.Disabled ignored -> null;
            case Tls.SystemTrust ignored -> null;
            case Tls.Custom(var certFile, var keyFile, var trustFile) -> {
                Transports.requireCompleteClientMutualTls(certFile, keyFile);
                yield ClientFactory.builder().tlsCustomizer(b -> {
                    if (certFile != null && keyFile != null) {
                        b.keyManager(certFile.toFile(), keyFile.toFile());
                    }
                    if (trustFile != null) {
                        b.trustManager(trustFile.toFile());
                    }
                }).build();
            }
            case Tls.Inline(var cert, var key, var trust) -> {
                Transports.requireCompleteClientMutualTls(cert, key);
                yield ClientFactory.builder().tlsCustomizer(b -> {
                    if (cert != null && key != null) {
                        b.keyManager(new ByteArrayInputStream(cert), new ByteArrayInputStream(key));
                    }
                    if (trust != null) {
                        b.trustManager(new ByteArrayInputStream(trust));
                    }
                }).build();
            }
            // Armeria's TLS runs on Netty, which has no SSLContext door: honour the trust manager only
            // (no mutual TLS through a caller-built SSLContext), matching the legacy gRPC transport.
            case Tls.SslContext(var ignoredContext, var trustManager) ->
                ClientFactory.builder().tlsCustomizer(b -> b.trustManager(trustManager)).build();
        };
    }
}
