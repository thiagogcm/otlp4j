package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.testing.conformance.GatewayUnderTest;
import dev.nthings.otlp4j.testing.conformance.OtlpHttpConformanceSuite;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.conformance.Scenario;

/// Executes the reusable OTLP/HTTP conformance suite against a real gateway that boots only the
/// HTTP transport. This class supplies the seam factory and names nothing else: every assertion
/// lives in the inherited abstract suite, and the only otlp4j runtime type in the module is reached
/// through [Otlp4jGatewayUnderTest].
final class HttpConformanceTest extends OtlpHttpConformanceSuite {

    @Override
    protected GatewayUnderTest boot(Scenario scenario, Protocol protocol) {
        return Otlp4jGatewayUnderTest.boot(scenario, Protocol.HTTP);
    }
}
