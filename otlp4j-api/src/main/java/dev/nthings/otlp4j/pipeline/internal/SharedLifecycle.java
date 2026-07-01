package dev.nthings.otlp4j.pipeline.internal;

import dev.nthings.otlp4j.pipeline.Lifecycle;

/// Internal wiring for a [Lifecycle] that several pipeline subscriptions can share, such as a
/// multi-signal exporter whose per-signal facets each carry the one channel's lifecycle.
///
/// A subscription calls [#retain()] once when it collects the resource, balancing it with the one
/// `shutdown` it issues at teardown. The underlying handle is released only when the last retained
/// owner shuts down, so one subscription tearing down cannot close the channel out from under
/// another. Not part of the public surface; implemented by the shared exporter and the count
/// connectors, and driven by the pipeline lifecycle collector.
public interface SharedLifecycle extends Lifecycle {

    /// Registers one more pipeline owner of this resource. Balanced by a later `shutdown`.
    void retain();
}
