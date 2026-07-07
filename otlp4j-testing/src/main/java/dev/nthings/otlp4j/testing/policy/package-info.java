/// The reusable exporter policy semantics suite and its neutral seam.
///
/// [dev.nthings.otlp4j.testing.policy.PolicySemanticsSuite] is an abstract JUnit class that pins how the
/// neutral RetryPolicy, BackoffPolicy, and TimeoutPolicy behave end to end over a real transport and a
/// programmed server. It drives only the neutral vocabulary in this package
/// ([dev.nthings.otlp4j.testing.policy.ProgrammedResponse], [dev.nthings.otlp4j.testing.policy.ExportProgram],
/// [dev.nthings.otlp4j.testing.policy.ExportObservation]) through the
/// [dev.nthings.otlp4j.testing.policy.PolicyExporterUnderTest] seam, so a concrete runner in
/// otlp4j-conformance supplies one seam implementation per transport and the same suite runs on both
/// legacy transports today and gates the future Armeria transport with no edit.
///
/// [dev.nthings.otlp4j.testing.policy.ProgrammedOtlpServer] is the small fake collector the seam boots
/// per export; it replays a script and records per-attempt arrivals so the suite can read attempt counts
/// and inter-attempt gaps.
package dev.nthings.otlp4j.testing.policy;
