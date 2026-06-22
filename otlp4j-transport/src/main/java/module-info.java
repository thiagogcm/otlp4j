/// OTLP/gRPC transport implementation.
///
/// This module maps between generated proto types and the public domain model, provides the API's
/// transport SPI, and exports no packages.
module dev.nthings.otlp4j.transport {
    requires dev.nthings.otlp4j.model;
    requires dev.nthings.otlp4j.api;
    requires dev.nthings.otlp4j.proto;
    requires com.google.protobuf;
    requires io.grpc;
    requires io.grpc.stub;
    // Netty transport (shaded artifact): the server binds a specific interface and applies the
    // hardening knobs via NettyServerBuilder, so it is now a compile-time, not runtime-only, dep.
    requires io.grpc.netty.shaded;
    requires org.slf4j;

    // Also declared in META-INF/services so the providers resolve when run on the class path.
    provides dev.nthings.otlp4j.spi.OtlpServerProvider
            with dev.nthings.otlp4j.internal.GrpcOtlpServerProvider;
    provides dev.nthings.otlp4j.spi.OtlpClientProvider
            with dev.nthings.otlp4j.internal.GrpcOtlpClientProvider;
}
