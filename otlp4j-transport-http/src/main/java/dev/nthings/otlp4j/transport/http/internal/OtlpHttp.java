package dev.nthings.otlp4j.transport.http.internal;

/// Shared OTLP/HTTP wire constants: the binary-protobuf content type and the standard per-signal
/// request paths, used by both the [HttpOtlpClient] (to build request URLs) and the
/// [HttpExchangeHandlers] (to register server contexts).
final class OtlpHttp {

    private OtlpHttp() {}

    /// The OTLP/HTTP binary-protobuf media type.
    static final String CONTENT_TYPE = "application/x-protobuf";

    static final String TRACES_PATH = "/v1/traces";
    static final String METRICS_PATH = "/v1/metrics";
    static final String LOGS_PATH = "/v1/logs";
    /// Profiles are a development signal; the path tracks the `v1development` collector convention.
    static final String PROFILES_PATH = "/v1development/profiles";
}
