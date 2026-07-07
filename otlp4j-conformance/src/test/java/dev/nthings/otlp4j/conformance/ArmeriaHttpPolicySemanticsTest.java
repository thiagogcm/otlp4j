package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.policy.PolicyExporterUnderTest;
import dev.nthings.otlp4j.testing.policy.PolicySemanticsSuite;
import org.junit.jupiter.api.Tag;

/// Executes the reusable policy semantics suite against a real OTLP/HTTP exporter on the consolidated
/// Armeria transport, the only production transport, driving a programmed HTTP server. It drives the
/// retry counts, backoff bounds, deadline behaviour, and `Retry-After` folding against Armeria, with
/// the total-deadline cancellation capability asserting the stronger behaviour here. This class
/// supplies the seam and names nothing else.
///
/// Tagged `policy-timing`: the deterministic counts, orderings, and outcomes are not flaky, but the
/// wall-clock upper bounds carry a fixed tolerance, so CI may grant this class a single automatic retry.
@Tag("policy-timing")
final class ArmeriaHttpPolicySemanticsTest extends PolicySemanticsSuite {

    @Override
    protected PolicyExporterUnderTest newSeam() {
        return new Otlp4jPolicyExporterUnderTest(Protocol.HTTP);
    }
}
