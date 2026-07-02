package dev.nthings.otlp4j.transport.common.internal;

import dev.nthings.otlp4j.config.Headers;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/// Per-export header resolution shared by the bundled transports.
public final class ClientHeaders {

    private ClientHeaders() {}

    /// Resolves the request headers for one export: the static `constant` headers overlaid by the
    /// per-export `supplier` (supplier wins per key). Returns `constant` unchanged when no
    /// supplier is set - it was already validated when the config was built. A supplier-provided
    /// map is re-validated on every resolve; an invalid header fails the export exceptionally and
    /// is not retried.
    ///
    /// @param constant the static, already-validated headers
    /// @param supplier the per-export header supplier, or null
    /// @return the resolved headers for this export
    public static Map<String, String> resolve(
            Map<String, String> constant, @Nullable Supplier<Map<String, String>> supplier) {
        if (supplier == null) {
            return constant;
        }
        var merged = new LinkedHashMap<String, String>(constant);
        // Only the supplier's entries need validating: the constants were validated at build.
        merged.putAll(Headers.requireValid(supplier.get()));
        return merged;
    }
}
