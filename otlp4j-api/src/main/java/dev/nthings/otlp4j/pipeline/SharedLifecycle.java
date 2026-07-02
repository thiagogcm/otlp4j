package dev.nthings.otlp4j.pipeline;

/// A [Lifecycle] that several pipeline subscriptions can share, such as a multi-signal exporter
/// whose per-signal facets each carry the one channel's lifecycle, or a connector that owns a
/// shared downstream.
///
/// The protocol is reference counting: a subscription calls [#retain()] exactly once when it
/// collects the resource, balanced by the one `shutdown` it issues at teardown. The underlying
/// handle is released only when the last retained owner shuts down, so one subscription tearing
/// down cannot close the channel out from under another.
///
/// Implement this when a resource is shared across subscriptions - a custom transport's exporter
/// wrapping one connection behind several facets, or a [dev.nthings.otlp4j.processor.Connector]
/// cascading to a shared downstream. The bundled shared exporter and connectors already do.
public interface SharedLifecycle extends Lifecycle {

    /// Registers one more pipeline owner of this resource. Balanced by a later `shutdown`.
    void retain();
}
