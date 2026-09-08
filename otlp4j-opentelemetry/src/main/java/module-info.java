/// Adapters between the OpenTelemetry Java SDK and the typed in-process otlp4j inputs.
@SuppressWarnings("requires-transitive-automatic")
module dev.nthings.otlp4j.opentelemetry {
    requires transitive dev.nthings.otlp4j.core;
    requires transitive io.opentelemetry.api;
    requires transitive io.opentelemetry.sdk.common;
    requires transitive io.opentelemetry.sdk.trace;
    requires transitive io.opentelemetry.sdk.metrics;
    requires transitive io.opentelemetry.sdk.logs;

    exports dev.nthings.otlp4j.opentelemetry;
}
