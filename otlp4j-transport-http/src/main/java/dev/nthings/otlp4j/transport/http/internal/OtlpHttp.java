package dev.nthings.otlp4j.transport.http.internal;

/// Shared OTLP/HTTP wire constants: content type and per-signal request paths, used by
/// [HttpOtlpClient] and [HttpExchangeHandlers].
final class OtlpHttp {

    private OtlpHttp() {}

    /// The OTLP/HTTP binary-protobuf media type.
    static final String CONTENT_TYPE = "application/x-protobuf";

    static final String TRACES_PATH = "/v1/traces";
    static final String METRICS_PATH = "/v1/metrics";
    static final String LOGS_PATH = "/v1/logs";
    /// Profiles are a development signal; the path follows the `v1development` convention.
    static final String PROFILES_PATH = "/v1development/profiles";
}
