/// Generated OpenTelemetry Protocol message and gRPC service code.
///
/// Packages are exported only to the transport module, keeping generated types out of the
/// user-facing API.
module dev.nthings.otlp4j.proto {
    // Protobuf and gRPC runtime APIs are used directly by the generated code here and
    // re-declared by the transport module, which is the only reader of this module — so they
    // need not be transitive.
    requires com.google.protobuf;
    requires io.grpc;
    requires io.grpc.stub;
    // The gRPC protobuf runtime and Guava are reached implicitly through generated *FutureStub
    // classes (which expose Guava's ListenableFuture) and the blocking call paths. The transport
    // module does not re-declare them, so they must stay transitive.
    requires transitive io.grpc.protobuf;
    requires transitive com.google.common;
    // failureaccess backs Guava's ListenableFuture at runtime but is named by nothing downstream
    // — only this module's own generated code needs it readable — so it is not transitive.
    requires com.google.common.util.concurrent.internal;

    exports io.opentelemetry.proto.common.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.resource.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.trace.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.metrics.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.logs.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.profiles.v1development to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.collector.trace.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.collector.metrics.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.collector.logs.v1 to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;
    exports io.opentelemetry.proto.collector.profiles.v1development to dev.nthings.otlp4j.codec, dev.nthings.otlp4j.transport.grpc, dev.nthings.otlp4j.transport.http;

    // processcontext.v1development is generated but intentionally not exported: ProcessContext is a
    // non-OTLP, memory-mapped sidecar (not collector-exchanged), so the transport has no mapper for
    // it. Kept encapsulated rather than excluded so update-protos stays a faithful upstream mirror.
}
