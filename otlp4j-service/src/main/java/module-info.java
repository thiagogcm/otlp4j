/// Typed in-process pipelines and the validated, JGraphT-backed multi-signal gateway runtime.
///
/// This module exports the gateway builder and runtime, the configuration model, and the validation
/// diagnostics. The internal service graph package is not exported, so no `org.jgrapht` type ever
/// crosses a module boundary; `org.jgrapht.core` is a real explicit module read internally only. The
/// one latent constraint, recorded for a future maintainer, is that `jgrapht-core` itself declares
/// `requires transitive` on two genuine automatic modules (`org.jheaps` and `org.apfloat`), so this
/// module transitively reads them two hops away. That produces no warning here, but it forecloses a
/// clean `jlink` image; if that ever matters, the internal graph can be reimplemented over a
/// hand-rolled adjacency structure behind the same surface.
module dev.nthings.otlp4j.service {
    requires transitive dev.nthings.otlp4j.core;
    requires org.jgrapht.core;
    requires static transitive org.jspecify;

    exports dev.nthings.otlp4j.service;
    exports dev.nthings.otlp4j.service.config;
    exports dev.nthings.otlp4j.service.validation;
}
