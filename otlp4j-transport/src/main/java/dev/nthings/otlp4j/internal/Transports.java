package dev.nthings.otlp4j.internal;

import java.net.InetSocketAddress;
import java.nio.file.Path;

/// Small plumbing shared by the OTLP/gRPC and OTLP/HTTP transports, so the two implementations agree
/// on bind-address resolution and client mutual-TLS validation rather than each carrying a copy.
final class Transports {

    private Transports() {}

    /// Resolves a receiver's bind host and port to a socket address. A wildcard host (empty,
    /// `0.0.0.0`, `::`, or its long form) binds every interface via the any-local-address; any other
    /// host binds that specific interface, so e.g. `127.0.0.1` yields a loopback-only receiver.
    static InetSocketAddress bindAddress(String bindHost, int port) {
        return isWildcardHost(bindHost)
                ? new InetSocketAddress(port)
                : new InetSocketAddress(bindHost, port);
    }

    private static boolean isWildcardHost(String host) {
        return host.isEmpty()
                || host.equals("0.0.0.0")
                || host.equals("::")
                || host.equals("0:0:0:0:0:0:0:0");
    }

    /// Rejects half-specified client mutual-TLS material: a certificate and key are only meaningful
    /// together, so a lone one is an error rather than a silent anonymous connection.
    static void requireCompleteClientMutualTls(Path certFile, Path keyFile) {
        if ((certFile == null) != (keyFile == null)) {
            throw new IllegalArgumentException(
                    "incomplete client mutual-TLS material: a certificate and key must be "
                            + "supplied together (got "
                            + (certFile != null ? "a certificate without a key" : "a key without a certificate")
                            + ")");
        }
    }
}
