/// Delivery machinery shared by components that acknowledge telemetry before it is delivered: the
/// bounded [dev.nthings.otlp4j.core.delivery.DeliveryQueue] with optional redelivery, and the
/// [dev.nthings.otlp4j.core.delivery.QueuedExporter] decorator that puts one in front of any exporter.
/// Like the rest of the contracts module it has no logging or transport dependency; losses are reported
/// to a listener the owning component supplies.
@NullMarked
package dev.nthings.otlp4j.core.delivery;

import org.jspecify.annotations.NullMarked;
