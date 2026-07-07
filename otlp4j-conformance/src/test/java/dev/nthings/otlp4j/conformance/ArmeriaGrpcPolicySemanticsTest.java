package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.policy.PolicyExporterUnderTest;
import dev.nthings.otlp4j.testing.policy.PolicySemanticsSuite;
import org.junit.jupiter.api.Tag;

/// Executes the reusable policy semantics suite against a real OTLP/gRPC exporter on the consolidated
/// Armeria transport, the only production transport, driving a programmed gRPC server. It drives the
/// retry counts, backoff bounds, deadline behaviour, negative-pushback stop-retry, and throttle
/// folding against Armeria. Because the Armeria seam declares it honours gRPC RetryInfo and cancels
/// retries at the total deadline, the capability-gated cases assert the stronger behaviour here
/// without any edit to the suite. This class supplies the seam and names nothing else.
///
/// Tagged `policy-timing`: the deterministic counts, orderings, and outcomes are not flaky, but the
/// wall-clock upper bounds carry a fixed tolerance, so CI may grant this class a single automatic retry.
@Tag("policy-timing")
final class ArmeriaGrpcPolicySemanticsTest extends PolicySemanticsSuite {

    @Override
    protected PolicyExporterUnderTest newSeam() {
        return new Otlp4jPolicyExporterUnderTest(Protocol.GRPC);
    }
}
