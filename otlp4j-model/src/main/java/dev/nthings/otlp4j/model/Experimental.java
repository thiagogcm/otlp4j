package dev.nthings.otlp4j.model;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/// Marks an API element as experimental.
///
/// Experimental elements may be removed or reshaped in a future minor release without notice.
/// Applied to the profiles signal, which currently targets OpenTelemetry `v1development`.
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({
    ElementType.TYPE,
    ElementType.METHOD,
    ElementType.CONSTRUCTOR,
    ElementType.FIELD,
    ElementType.PACKAGE,
    ElementType.RECORD_COMPONENT
})
public @interface Experimental {

    /// Optional note explaining the experimental nature.
    String value() default "";
}
