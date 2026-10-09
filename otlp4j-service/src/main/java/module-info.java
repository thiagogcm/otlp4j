/// Typed in-process pipelines and the validated multi-signal gateway runtime.
///
/// This module exports the gateway builder and runtime, the configuration model, and the validation
/// diagnostics. The internal service graph package is not exported, so no graph vertex or edge type
/// crosses a module boundary. The graph is held in this module's own adjacency structure, so the
/// module reads only the core contracts and adds no third-party runtime dependency.
module dev.nthings.otlp4j.service {
    requires transitive dev.nthings.otlp4j.core;
    requires static transitive org.jspecify;

    exports dev.nthings.otlp4j.service;
    exports dev.nthings.otlp4j.service.config;
    exports dev.nthings.otlp4j.service.validation;
}
