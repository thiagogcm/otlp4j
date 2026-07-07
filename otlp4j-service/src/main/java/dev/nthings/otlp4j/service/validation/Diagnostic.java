package dev.nthings.otlp4j.service.validation;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import java.util.Optional;

/// One validation finding: a classified [DiagnosticCode], a human-readable message, and the optional
/// component and pipeline the finding is attributed to. All findings for a configuration are
/// collected in a single pass and reported together through a [GraphValidationException], so a caller
/// sees every problem at once rather than fixing one and rediscovering the next.
///
/// @param code      the classified reason
/// @param message   a human-readable explanation
/// @param component the component the finding is about, if any
/// @param pipeline  the pipeline the finding is about, if any
public record Diagnostic(
        DiagnosticCode code, String message, Optional<ComponentId> component, Optional<PipelineId> pipeline) {

    /// Validates the code and message and normalizes null optionals to empty.
    public Diagnostic {
        if (code == null) {
            throw new IllegalArgumentException("diagnostic code must be non-null");
        }
        if (message == null) {
            throw new IllegalArgumentException("diagnostic message must be non-null");
        }
        component = component == null ? Optional.empty() : component;
        pipeline = pipeline == null ? Optional.empty() : pipeline;
    }

    /// A diagnostic attributed to a component within a pipeline.
    ///
    /// @param code      the classified reason
    /// @param message   the explanation
    /// @param component the component
    /// @param pipeline  the pipeline
    /// @return the diagnostic
    public static Diagnostic of(
            DiagnosticCode code, String message, ComponentId component, PipelineId pipeline) {
        return new Diagnostic(code, message, Optional.of(component), Optional.of(pipeline));
    }

    /// A diagnostic attributed to a component with no pipeline.
    ///
    /// @param code      the classified reason
    /// @param message   the explanation
    /// @param component the component
    /// @return the diagnostic
    public static Diagnostic ofComponent(DiagnosticCode code, String message, ComponentId component) {
        return new Diagnostic(code, message, Optional.of(component), Optional.empty());
    }

    /// A diagnostic attributed to a pipeline with no single component.
    ///
    /// @param code     the classified reason
    /// @param message  the explanation
    /// @param pipeline the pipeline
    /// @return the diagnostic
    public static Diagnostic ofPipeline(DiagnosticCode code, String message, PipelineId pipeline) {
        return new Diagnostic(code, message, Optional.empty(), Optional.of(pipeline));
    }

    @Override
    public String toString() {
        var where = new StringBuilder();
        component.ifPresent(c -> where.append(" component=").append(c));
        pipeline.ifPresent(p -> where.append(" pipeline=").append(p));
        return code + ": " + message + where;
    }
}
