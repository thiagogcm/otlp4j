/// Generated OpenTelemetry Protocol message and gRPC service code.
///
/// Packages are exported only to `dev.nthings.otlp4j.transport`, keeping generated types out of
/// the user-facing API.
module dev.nthings.otlp4j.proto {
    // protobuf-java, io.grpc and io.grpc.stub are used directly by the generated code here and
    // re-declared by the transport module, which is the only reader of this module — so they
    // need not be transitive.
    requires com.google.protobuf;
    requires io.grpc;
    requires io.grpc.stub;
    // io.grpc.protobuf and Guava are reached implicitly through the generated gRPC *FutureStub
    // classes (which expose Guava's ListenableFuture) and the blocking call paths. The transport
    // module does not re-declare them, so they must stay transitive.
    requires transitive io.grpc.protobuf;
    requires transitive com.google.common;
    // failureaccess backs Guava's ListenableFuture at runtime but is named by nothing downstream
    // — only this module's own generated code needs it readable — so it is not transitive.
    requires com.google.common.util.concurrent.internal;

    exports io.opentelemetry.proto.common.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.resource.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.trace.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.metrics.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.logs.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.profiles.v1development to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.collector.trace.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.collector.metrics.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.collector.logs.v1 to dev.nthings.otlp4j.transport;
    exports io.opentelemetry.proto.collector.profiles.v1development to dev.nthings.otlp4j.transport;
}
