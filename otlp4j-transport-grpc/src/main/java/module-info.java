/// The OTLP/gRPC transport: `OtlpGrpcExporter` and `OtlpGrpcReceiver`, built on
/// gRPC + Netty. The only module that pulls in `grpc-netty`, so an HTTP-only
/// deployment never carries Netty.
module dev.nthings.otlp4j.transport.grpc {
    requires dev.nthings.otlp4j.model;
    // The graph factories name the core component and signal contracts directly.
    requires dev.nthings.otlp4j.core;
    requires transitive dev.nthings.otlp4j.transport.common;
    requires dev.nthings.otlp4j.codec;
    requires dev.nthings.otlp4j.proto;
    // Request sizing for admission control (MessageLite.getSerializedSize).
    requires com.google.protobuf;
    requires io.grpc;
    requires io.grpc.stub;
    requires io.grpc.netty;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.grpc;
}
