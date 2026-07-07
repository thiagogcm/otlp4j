/// The built-in memory limiter processor and its configuration. The processor samples a used-memory
/// gauge every check interval through a shared limiter core and reacts to the sampled state: in the
/// hard state it refuses ingress with a retryable throttled result carrying a retry-after hint, so the
/// receiver emits gRPC pushback or an HTTP `Retry-After` header; in the soft state it requests a
/// best-effort collection at most once per check interval and forwards; in the normal state it forwards
/// unchanged. Recovery is automatic once used memory drops back under the soft threshold. The soft
/// threshold is the limit minus the spike band and the hard threshold is the limit, mirroring the
/// Collector's memory limiter.
package dev.nthings.otlp4j.components.memory;
