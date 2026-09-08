package dev.nthings.otlp4j.transport;

import java.nio.file.Path;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;

/// TLS configuration for an OTLP transport.
///
/// Sealed family covering disabled TLS, system-trust TLS, and two ways to supply custom credentials:
/// PEM files ([Custom]) or in-memory PEM bytes ([Inline]). The bundled transports honour every client
/// variant; a server honours [Custom] and [Inline] (which supply its certificate and key) and rejects
/// [SystemTrust], as a server has no certificate of its own.
public sealed interface Tls
        permits Tls.Disabled, Tls.SystemTrust, Tls.Custom, Tls.Inline {

    /// Plaintext transport (no TLS).
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

    /// Client-side TLS trusting in-memory PEM `trust` bytes; no client certificate. In-memory twin
    /// of [#trust(Path)] for material loaded from a secret manager or vault.
    static Tls trust(byte[] trust) {
        return new Inline(null, null, trust);
    }

    /// TLS from in-memory PEM bytes: the in-memory twin of [#custom(Path, Path, Path)]. On a server
    /// the certificate and key are required; on a client they enable mutual TLS and `trust`, when
    /// non-null, overrides the default trust store.
    static Tls custom(byte[] cert, byte[] key, byte @Nullable [] trust) {
        return new Inline(cert, key, trust);
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

    /// In-memory PEM material; prefer the [Tls#custom(Path, Path, Path)] and [Tls#trust(Path)] byte
    /// factories. Arrays are copied on construction and on every accessor, preserving the
    /// configuration's immutability.
    record Inline(byte @Nullable [] cert, byte @Nullable [] key, byte @Nullable [] trust) implements Tls {
        public Inline {
            cert = cert == null ? null : cert.clone();
            key = key == null ? null : key.clone();
            trust = trust == null ? null : trust.clone();
        }
        @Override
        public byte @Nullable [] cert() {
            return cert == null ? null : cert.clone();
        }

        @Override
        public byte @Nullable [] key() {
            return key == null ? null : key.clone();
        }

        @Override
        public byte @Nullable [] trust() {
            return trust == null ? null : trust.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Inline that
                    && Arrays.equals(cert, that.cert)
                    && Arrays.equals(key, that.key)
                    && Arrays.equals(trust, that.trust);
        }

        @Override
        public int hashCode() {
            var result = Arrays.hashCode(cert);
            result = 31 * result + Arrays.hashCode(key);
            return 31 * result + Arrays.hashCode(trust);
        }
    }
}
