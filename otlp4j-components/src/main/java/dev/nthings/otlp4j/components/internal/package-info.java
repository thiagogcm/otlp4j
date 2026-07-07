/// Module-internal machinery shared by the built-in components, not part of any public surface. This
/// package is not exported by the module, so the bounded delivery queue, the memory limiter core, the
/// size selectors, and the shared routing and filtering match engine here are reusable within the
/// module without becoming a contract a downstream consumer can depend on.
package dev.nthings.otlp4j.components.internal;
