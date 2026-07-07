package dev.nthings.otlp4j.service;

/// The entry point to the graph runtime. [#builder] returns a small fluent assembler that collects
/// components and pipelines and compiles to a [dev.nthings.otlp4j.service.config.GatewayConfig], the
/// single intermediate form every construction path passes through.
public final class Gateway {

    private Gateway() {}

    /// A fresh fluent assembler for a gateway.
    ///
    /// @return the assembler
    public static GatewayAssembler builder() {
        return new GatewayAssembler();
    }
}
