package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.testing.conformance.GatewayUnderTest;
import dev.nthings.otlp4j.testing.conformance.OtlpHttpConformanceSuite;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.conformance.Scenario;

/// Executes the reusable OTLP/HTTP conformance suite against a real gateway that boots the
/// consolidated Armeria transport, the only production transport. It re-drives every pinned wire
/// behaviour against Armeria. This class supplies the seam factory and names nothing else.
final class ArmeriaHttpConformanceTest extends OtlpHttpConformanceSuite {

    @Override
    protected GatewayUnderTest boot(Scenario scenario, Protocol protocol) {
        return Otlp4jGatewayUnderTest.boot(scenario, Protocol.HTTP);
    }
}
