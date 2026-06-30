package dev.nthings.otlp4j.codec;

import java.net.InetSocketAddress;
import java.nio.file.Path;

/// Shared plumbing for OTLP/gRPC and OTLP/HTTP transports for
/// bind-address resolution and client mutual-TLS validation.
public final class Transports {

    private Transports() {}

    /// Resolves a receiver's bind host and port to an [InetSocketAddress]. A wildcard host
    /// (empty, `0.0.0.0`, `::`, or its long form) binds every interface; any other host
    /// binds that specific interface.
    public static InetSocketAddress bindAddress(String bindHost, int port) {
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

    /// Rejects half-specified client mutual-TLS material. A certificate and key must be
    /// supplied together.
    public static void requireCompleteClientMutualTls(Path certFile, Path keyFile) {
        if ((certFile == null) != (keyFile == null)) {
            throw new IllegalArgumentException(
                    "incomplete client mutual-TLS material: a certificate and key must be "
                            + "supplied together (got "
                            + (certFile != null ? "a certificate without a key" : "a key without a certificate")
                            + ")");
        }
    }
}
