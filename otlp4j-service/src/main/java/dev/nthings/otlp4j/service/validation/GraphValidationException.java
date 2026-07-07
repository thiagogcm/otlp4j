package dev.nthings.otlp4j.service.validation;

import java.util.List;

/// The exception a [dev.nthings.otlp4j.service.GatewayBuilder] throws when a configuration fails
/// validation. It carries the full list of [Diagnostic]s so a caller sees every problem at once.
/// Validation always runs, and this exception is always thrown, before any component is constructed,
/// so a rejected configuration never leaves a half-built gateway behind. This type reports otlp4j
/// diagnostics only; a JGraphT cycle exception is caught internally and converted to a
/// [DiagnosticCode#CYCLE_DETECTED] diagnostic rather than escaping.
public final class GraphValidationException extends RuntimeException {

    private final transient List<Diagnostic> diagnostics;

    /// Builds the exception from a non-empty list of diagnostics.
    ///
    /// @param diagnostics the collected diagnostics, at least one
    public GraphValidationException(List<Diagnostic> diagnostics) {
        super(summarize(diagnostics));
        this.diagnostics = List.copyOf(diagnostics);
    }

    /// The collected diagnostics, in the order they were found.
    ///
    /// @return the immutable diagnostic list
    public List<Diagnostic> diagnostics() {
        return diagnostics;
    }

    private static String summarize(List<Diagnostic> diagnostics) {
        if (diagnostics == null || diagnostics.isEmpty()) {
            throw new IllegalArgumentException("a validation exception requires at least one diagnostic");
        }
        var sb = new StringBuilder("gateway configuration is invalid (")
                .append(diagnostics.size())
                .append(diagnostics.size() == 1 ? " diagnostic):" : " diagnostics):");
        for (var d : diagnostics) {
            sb.append("\n  - ").append(d);
        }
        return sb.toString();
    }
}
