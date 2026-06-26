package dev.nthings.otlp4j.config;

import java.nio.file.Path;
import org.jspecify.annotations.Nullable;

/// TLS configuration for an OTLP transport.
///
/// Sealed family covering the three useful states: TLS disabled, TLS using the JVM's default
/// trust store, and a fully custom credential bundle. The bundled OTLP/gRPC transport honours
/// all three on the client; the server honours [Disabled] and [Custom] (which supplies its
/// certificate and key) and rejects [SystemTrust], as a server has no certificate of its own.
public sealed interface Tls permits Tls.Disabled, Tls.SystemTrust, Tls.Custom {

    /// Plaintext transport — no TLS.
    static Tls disabled() {
        return Disabled.INSTANCE;
    }

    /// TLS using the JVM's default trust store; no client certificate.
    static Tls systemTrust() {
        return SystemTrust.INSTANCE;
    }

    /// Client-side TLS trusting `trustFile` for the server's certificate; no client certificate.
    static Tls trust(Path trustFile) {
        return new Custom(null, null, trustFile);
    }

    /// TLS with a caller-supplied certificate, key, and (optional) trust material. On a server the
    /// certificate and key are required; on a client they enable mutual TLS and `trustFile`, when
    /// non-null, overrides the default trust store.
    static Tls custom(Path certFile, Path keyFile, @Nullable Path trustFile) {
        return new Custom(certFile, keyFile, trustFile);
    }

    /// Prefer [Tls#disabled()].
    record Disabled() implements Tls {
        private static final Disabled INSTANCE = new Disabled();
    }

    /// Prefer [Tls#systemTrust()].
    record SystemTrust() implements Tls {
        private static final SystemTrust INSTANCE = new SystemTrust();
    }

    /// Prefer the [Tls#custom(Path, Path, Path)] and [Tls#trust(Path)] factories.
    record Custom(@Nullable Path certFile, @Nullable Path keyFile, @Nullable Path trustFile) implements Tls {}
}
