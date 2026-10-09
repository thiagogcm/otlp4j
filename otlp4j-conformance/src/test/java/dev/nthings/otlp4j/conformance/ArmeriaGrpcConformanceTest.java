package dev.nthings.otlp4j.conformance;

import dev.nthings.otlp4j.testing.conformance.GatewayUnderTest;
import dev.nthings.otlp4j.testing.conformance.OtlpGrpcConformanceSuite;
import dev.nthings.otlp4j.testing.conformance.Protocol;
import dev.nthings.otlp4j.testing.conformance.Scenario;
import org.junit.jupiter.api.AfterAll;

/// Executes the reusable OTLP/gRPC conformance suite against a real gateway that boots the
/// consolidated Armeria transport, the only production transport. It re-drives every pinned wire
/// behaviour against Armeria. This class supplies the seam factory and names nothing else.
final class ArmeriaGrpcConformanceTest extends OtlpGrpcConformanceSuite {

    @Override
    protected GatewayUnderTest boot(Scenario scenario, Protocol protocol) {
        return Otlp4jGatewayUnderTest.boot(scenario, Protocol.GRPC);
    }

    @AfterAll
    static void stopGateways() {
        Otlp4jGatewayUnderTest.stopClosed();
    }
}
