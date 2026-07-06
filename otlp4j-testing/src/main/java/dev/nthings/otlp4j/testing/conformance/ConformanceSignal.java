package dev.nthings.otlp4j.testing.conformance;

/// The four OTLP telemetry signals, expressed as a neutral vocabulary the conformance suite owns.
///
/// This is the deliberate replacement for the retired model signal enum: the suite must not name
/// any fluent API or model type the redesign deletes, so it carries its own signal identity plus
/// the per-signal OTLP/HTTP request path, including the `v1development` profiles oddball.
public enum ConformanceSignal {

    /// Trace spans, posted to `/v1/traces`.
    TRACES("/v1/traces", "traces"),

    /// Metric points, posted to `/v1/metrics`.
    METRICS("/v1/metrics", "metrics"),

    /// Log records, posted to `/v1/logs`.
    LOGS("/v1/logs", "logs"),

    /// Profiles, a development signal posted to `/v1development/profiles`.
    PROFILES("/v1development/profiles", "profiles");

    private final String httpPath;
    private final String label;

    ConformanceSignal(String httpPath, String label) {
        this.httpPath = httpPath;
        this.label = label;
    }

    /// The OTLP/HTTP request path this signal is served on, appended to the gateway base URI.
    ///
    /// @return the absolute request path, for example `/v1/traces`
    public String httpPath() {
        return httpPath;
    }

    /// A short human label for test display names.
    ///
    /// @return the lowercase signal label, for example `traces`
    public String label() {
        return label;
    }
}
