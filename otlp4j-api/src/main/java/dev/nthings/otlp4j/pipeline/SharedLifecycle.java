package dev.nthings.otlp4j.pipeline;

import java.time.Duration;

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
/// wrapping one connection behind several facets, or a connector cascading to a shared downstream.
/// The bundled shared exporter and connectors already do.
public interface SharedLifecycle extends Lifecycle {

    /// Registers one more pipeline owner of this resource. Balanced by a later [#shutdown(Duration)]
    /// or [#release()].
    void retain();

    /// Drops one owner's stake WITHOUT draining: decrements the reference count, never closing the
    /// underlying resource even if it reaches zero.
    ///
    /// The symmetric counterpart to [#retain()] for an owner that detaches but is not the one to
    /// tear the resource down. The last owner that means to close it calls
    /// [Lifecycle#shutdown(Duration)] instead, which decrements and drains on the last release.
    ///
    /// WARNING: if EVERY owner calls `release()` and none calls [Lifecycle#shutdown(Duration)], the
    /// resource is never physically closed; the last owner must `shutdown()`.
    void release();
}
