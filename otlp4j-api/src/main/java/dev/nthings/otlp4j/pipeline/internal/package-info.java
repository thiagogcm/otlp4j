/// Internal pipeline wiring, excluded from the compatibility promise.
/// [SharedLifecycle] lets several subscriptions share one resource's lifecycle through a
/// retain/release count, so a shared exporter's channel closes only on the last release. Exported
/// only to the transport SPI, which supplies the shared exporter implementation.
@NullMarked
package dev.nthings.otlp4j.pipeline.internal;

import org.jspecify.annotations.NullMarked;
