package dev.nthings.otlp4j.transport.internal;

/// Shared OTLP/HTTP wire constants: the binary-protobuf content type and the per-signal request
/// paths, used by the Armeria HTTP server routes and the Armeria HTTP client. Keeping these constants
/// shared ensures both sides address the same OTLP endpoints.
final class OtlpHttpPaths {

    private OtlpHttpPaths() {}

    /// The OTLP/HTTP binary-protobuf media type.
    static final String CONTENT_TYPE = "application/x-protobuf";

    static final String TRACES_PATH = "/v1/traces";
    static final String METRICS_PATH = "/v1/metrics";
    static final String LOGS_PATH = "/v1/logs";
    /// Profiles are a development signal; the path follows the `v1development` convention.
    static final String PROFILES_PATH = "/v1development/profiles";
}
