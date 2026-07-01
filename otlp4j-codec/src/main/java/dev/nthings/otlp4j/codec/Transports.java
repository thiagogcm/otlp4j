package dev.nthings.otlp4j.codec;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/// Shared plumbing for OTLP/gRPC and OTLP/HTTP transports for bind-address resolution, client
/// mutual-TLS validation, and per-export header resolution.
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

    /// Rejects half-specified in-memory client mutual-TLS material. A certificate and key must be
    /// supplied together.
    public static void requireCompleteClientMutualTls(byte[] certData, byte[] keyData) {
        if ((certData == null) != (keyData == null)) {
            throw new IllegalArgumentException(
                    "incomplete client mutual-TLS material: a certificate and key must be "
                            + "supplied together (got "
                            + (certData != null ? "a certificate without a key" : "a key without a certificate")
                            + ")");
        }
    }

    /// Resolves the request headers for one export: the static `constant` headers overlaid by the
    /// per-export `supplier` (supplier wins per key). Returns `constant` unchanged when no supplier
    /// is set.
    public static Map<String, String> resolveHeaders(
            Map<String, String> constant, Supplier<Map<String, String>> supplier) {
        if (supplier == null) {
            return constant;
        }
        var merged = new LinkedHashMap<String, String>(constant);
        merged.putAll(supplier.get());
        return merged;
    }
}
