/// OTLP transport implementations (gRPC and HTTP).
///
/// This module maps between generated proto types and the public domain model, provides the API's
/// transport SPI for both OTLP/gRPC and OTLP/HTTP, and exports no packages. The HTTP transport uses
/// only the JDK (`java.net.http` client, `jdk.httpserver` server) — no extra third-party deps.
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
    // JDK HTTP transport: the client and server for OTLP/HTTP.
    requires java.net.http;
    requires jdk.httpserver;
    requires org.slf4j;

    // Also declared in META-INF/services so the providers resolve when run on the class path. Both
    // protocols' providers coexist; the API selects one by Protocol.
    provides dev.nthings.otlp4j.spi.OtlpServerProvider
            with dev.nthings.otlp4j.internal.GrpcOtlpServerProvider,
                    dev.nthings.otlp4j.internal.HttpOtlpServerProvider;
    provides dev.nthings.otlp4j.spi.OtlpClientProvider
            with dev.nthings.otlp4j.internal.GrpcOtlpClientProvider,
                    dev.nthings.otlp4j.internal.HttpOtlpClientProvider;
}
