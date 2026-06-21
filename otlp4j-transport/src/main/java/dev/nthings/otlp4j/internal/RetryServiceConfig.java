package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.RetryPolicy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Builds the gRPC channel service config (`defaultServiceConfig`) that maps a [RetryPolicy] onto
/// gRPC's native retry subsystem.
///
/// One `methodConfig` entry is emitted per OTLP service so the retry policy applies to all four
/// collector services without relying on the catch-all default-name behaviour. The retryable
/// status codes are the set the OTLP/gRPC spec recommends clients retry.
///
/// **Internal.** Part of the transport layer; not public API.
final class RetryServiceConfig {

    private static final List<String> RETRYABLE_STATUS_CODES = List.of(
            "CANCELLED",
            "DEADLINE_EXCEEDED",
            "RESOURCE_EXHAUSTED",
            "ABORTED",
            "OUT_OF_RANGE",
            "UNAVAILABLE",
            "DATA_LOSS");

    private RetryServiceConfig() {}

    /// Returns the channel service-config map for `policy`, scoped to `serviceNames`.
    static Map<String, Object> build(RetryPolicy policy, List<String> serviceNames) {
        var retryPolicy = new LinkedHashMap<String, Object>();
        retryPolicy.put("maxAttempts", (double) policy.maxAttempts());
        retryPolicy.put("initialBackoff", seconds(policy.initialBackoff()));
        retryPolicy.put("maxBackoff", seconds(policy.maxBackoff()));
        retryPolicy.put("backoffMultiplier", 2.0d);
        retryPolicy.put("retryableStatusCodes", RETRYABLE_STATUS_CODES);

        var methodConfigs = new ArrayList<Map<String, Object>>(serviceNames.size());
        for (var service : serviceNames) {
            var entry = new LinkedHashMap<String, Object>();
            entry.put("name", List.of(Map.of("service", service)));
            entry.put("retryPolicy", retryPolicy);
            methodConfigs.add(entry);
        }
        return Map.of("methodConfig", methodConfigs);
    }

    /// Renders a duration as the decimal-seconds-with-`s`-suffix form gRPC service configs expect.
    private static String seconds(Duration d) {
        return (d.toNanos() / 1_000_000_000.0) + "s";
    }
}
