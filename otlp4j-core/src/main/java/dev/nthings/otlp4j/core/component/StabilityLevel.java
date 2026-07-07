package dev.nthings.otlp4j.core.component;

/// How far along the stability ladder a component or signal ships, declared honestly during the 0.x
/// window so operators and the profiles-support machinery can see what is production-ready. The
/// order is increasing maturity: `DEVELOPMENT` is in-progress and may change or be removed, `ALPHA`
/// and `BETA` are progressively more settled, and `STABLE` carries the compatibility guarantees a
/// released component makes.
public enum StabilityLevel {

    /// In active development; the surface may change or be removed without notice.
    DEVELOPMENT,

    /// Early and usable, but the surface is still expected to change.
    ALPHA,

    /// Settling; breaking changes are avoided but still possible.
    BETA,

    /// Released with the compatibility guarantees a stable component makes.
    STABLE
}
