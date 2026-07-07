package dev.nthings.otlp4j.telemetry;

import java.util.function.LongSupplier;

/// The shared no-op [GatewayMetrics]: every instrument does nothing, so the default self-observability
/// posture costs a method call and no allocation. A component that is handed this registry allocates
/// no per-instrument state and its `counter`/`gauge` calls resolve to shared singletons.
final class NoopGatewayMetrics implements GatewayMetrics {

    static final NoopGatewayMetrics INSTANCE = new NoopGatewayMetrics();

    private static final LongCounter NO_OP_COUNTER = delta -> {};

    private static final Registration NO_OP_REGISTRATION = () -> {};

    private NoopGatewayMetrics() {}

    @Override
    public LongCounter counter(String name, String description) {
        return NO_OP_COUNTER;
    }

    @Override
    public Registration gauge(String name, String description, LongSupplier value) {
        return NO_OP_REGISTRATION;
    }
}
