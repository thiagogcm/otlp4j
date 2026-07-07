/// The validation diagnostics the graph builder reports before any component is constructed.
///
/// A [dev.nthings.otlp4j.service.validation.Diagnostic] pairs a classified
/// [dev.nthings.otlp4j.service.validation.DiagnosticCode] with a message and the component or
/// pipeline it is about. All findings for one configuration are collected in a single pass and
/// reported together through a [dev.nthings.otlp4j.service.validation.GraphValidationException], so a
/// caller sees every problem at once and a rejected configuration never leaves a half-built gateway
/// behind.
@NullMarked
package dev.nthings.otlp4j.service.validation;

import org.jspecify.annotations.NullMarked;
