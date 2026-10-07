/// Adapters between the OpenTelemetry Java SDK and the typed in-process otlp4j inputs.
// The OpenTelemetry SDK publishes automatic modules; re-exporting their public interfaces to callers is
// intentional, so javac's requires-transitive-automatic warning is suppressed here.
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
