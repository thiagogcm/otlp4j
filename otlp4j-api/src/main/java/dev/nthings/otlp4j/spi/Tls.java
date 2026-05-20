package dev.nthings.otlp4j.spi;

import java.nio.file.Path;

/// TLS configuration for an OTLP transport.
///
/// Sealed family covering the three useful states: TLS disabled, TLS using the JVM's default
/// trust store, and a fully custom credential bundle. The current OTLP/gRPC transport ships
/// with [Disabled] only; the other variants exist on the SPI so alternate transports and a
/// future TLS implementation can adopt them without an SPI break.
public sealed interface Tls permits Tls.Disabled, Tls.SystemTrust, Tls.Custom {

    /// Plaintext transport — no TLS.
    record Disabled() implements Tls {

        private static final Disabled INSTANCE = new Disabled();

        public static Disabled instance() {
            return INSTANCE;
        }
    }

    /// TLS using the JVM's default trust store; no client certificate.
    record SystemTrust() implements Tls {

        private static final SystemTrust INSTANCE = new SystemTrust();

        public static SystemTrust instance() {
            return INSTANCE;
        }
    }

    /// TLS with caller-supplied certificate, key, and (optional) trust material.
    record Custom(Path certFile, Path keyFile, Path trustFile) implements Tls {}
}
